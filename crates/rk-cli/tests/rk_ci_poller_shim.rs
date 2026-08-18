//! TKT-01M08HB56NRQ72ZZ4W6JKQ760Y (C4) — proves the exact `rk ingest event`
//! invocations `scripts/rk-ci-poller.py` builds (see its
//! `build_ingest_args`) are accepted by the real ingest pipeline with the
//! dedup and transition semantics the ticket's AC requires. The shim itself
//! is exercised offline via `--dry-run --fixture` (see its OPERATOR SETUP
//! docstring); this test proves the commands it would run, once GitHub
//! credentials are live, actually work end to end against the daemon.

use rk_core::config::{Config, IngestSourceConfig};
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::process::Command;
use std::time::Duration;

fn rk(layout: &Layout, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(args)
        .env("RK_HOME", layout.home())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .env_remove("RK_TASK")
        .env_remove("RK_REPO")
        .env_remove("RK_ROLE")
        .env_remove("RK_BRANCH")
        .env_remove("RK_WORKTREE")
        .output()
        .unwrap()
}

async fn rk_async(layout: &Layout, args: Vec<&'static str>) -> std::process::Output {
    let layout = layout.clone();
    tokio::task::spawn_blocking(move || rk(&layout, &args))
        .await
        .unwrap()
}

fn config() -> Config {
    let mut config = Config::default();
    config.ingest.sources = vec![IngestSourceConfig {
        name: "github-ci".into(),
        allowed_kinds: vec!["ci_failed".into(), "ci_recovered".into()],
        ..Default::default()
    }];
    config
}

async fn start() -> (
    tempfile::TempDir,
    Layout,
    tokio::task::JoinHandle<rk_core::Result<()>>,
) {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::at(dir.path());
    let daemon = Daemon::new(layout.clone(), &config()).unwrap();
    let handle = tokio::spawn(daemon.run());
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if Client::connect_as_operator(&layout).await.is_ok() {
            return (dir, layout, handle);
        }
    }
    panic!("daemon did not start");
}

async fn stop(layout: &Layout, handle: tokio::task::JoinHandle<rk_core::Result<()>>) {
    let mut client = Client::connect_as_operator(layout).await.unwrap();
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

// Mirrors scripts/rk-ci-poller.py's build_ingest_args() output exactly for
// the run-failure.json / run-recovery.json fixtures: same source, same
// delivery-id shape (`{run_id}-{run_attempt}`), same required correlation
// fields (repo/branch/workflow/job/commit-sha).
fn poller_args(
    kind: &'static str,
    delivery_id: &'static str,
    summary: &'static str,
    sha: &'static str,
) -> Vec<&'static str> {
    vec![
        "--json",
        "ingest",
        "event",
        "--source",
        "github-ci",
        "--kind",
        kind,
        "--delivery-id",
        delivery_id,
        "--summary",
        summary,
        "--repo",
        "rat-kingdom",
        "--branch",
        "main",
        "--workflow",
        "ci",
        "--job",
        "ci",
        "--commit-sha",
        sha,
        "--attr",
        "run_id=100001",
    ]
}

#[tokio::test]
async fn test_poller_shaped_ci_failed_call_is_accepted_with_a_transition() {
    let (_dir, layout, handle) = start().await;
    let output = rk_async(
        &layout,
        poller_args(
            "ci_failed",
            "100001-1",
            "ci failure on main (abc123def456)",
            "abc123def456abc123def456abc123def456ab",
        ),
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["accepted"], true);
    assert_eq!(value["receipt"]["delivery_id"], "100001-1");
    assert_eq!(
        value["receipt"]["transition_emitted"], true,
        "first observation of this (repo, branch, workflow) subject must emit a transition"
    );
    stop(&layout, handle).await;
}

#[tokio::test]
async fn test_poller_reposting_the_same_run_attempt_is_a_dedup_no_op() {
    let (_dir, layout, handle) = start().await;
    let args = poller_args(
        "ci_failed",
        "100001-1",
        "ci failure on main (abc123def456)",
        "abc123def456abc123def456abc123def456ab",
    );

    let first = rk_async(&layout, args.clone()).await;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_value: Value = serde_json::from_slice(&first.stdout).unwrap();

    // Same run_id, same run_attempt -> same delivery_id, exactly as a
    // re-poll of an unfinished GitHub Actions run would produce.
    let second = rk_async(&layout, args).await;
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_value: Value = serde_json::from_slice(&second.stdout).unwrap();

    assert_eq!(second_value["accepted"], true);
    assert_eq!(
        first_value["receipt"], second_value["receipt"],
        "a re-poll of the same run/attempt must return the original receipt unchanged, not project new state"
    );
    stop(&layout, handle).await;
}

#[tokio::test]
async fn test_poller_shaped_ci_recovered_call_emits_a_recovery_transition() {
    // The daemon's CI subject key includes commit_sha (store.rs sdlc_key),
    // so a plain new commit is always a first-time subject observation, not
    // a same-subject transition. The scenario that genuinely exercises the
    // failure -> success transition path is GitHub's "re-run failed jobs":
    // same run_id, same commit_sha, run_attempt increments. That is what
    // this test simulates.
    let (_dir, layout, handle) = start().await;

    let failed = rk_async(
        &layout,
        poller_args(
            "ci_failed",
            "100001-1",
            "ci failure on main (abc123def456)",
            "abc123def456abc123def456abc123def456ab",
        ),
    )
    .await;
    assert!(
        failed.status.success(),
        "{}",
        String::from_utf8_lossy(&failed.stderr)
    );

    let recovered = rk_async(
        &layout,
        poller_args(
            "ci_recovered",
            "100001-2",
            "ci success on main (abc123def456)",
            "abc123def456abc123def456abc123def456ab",
        ),
    )
    .await;
    assert!(
        recovered.status.success(),
        "{}",
        String::from_utf8_lossy(&recovered.stderr)
    );
    let recovered_value: Value = serde_json::from_slice(&recovered.stdout).unwrap();
    assert_eq!(recovered_value["accepted"], true);
    assert_eq!(
        recovered_value["receipt"]["transition_emitted"], true,
        "failure -> success on the same subject must emit a recovery transition"
    );

    let state = rk_async(
        &layout,
        vec![
            "--json",
            "ingest",
            "state",
            "--source",
            "github-ci",
            "--repo",
            "rat-kingdom",
        ],
    )
    .await;
    assert!(
        state.status.success(),
        "{}",
        String::from_utf8_lossy(&state.stderr)
    );
    let state_value: Value = serde_json::from_slice(&state.stdout).unwrap();
    assert_eq!(
        state_value["facts"][0]["payload"]["current"]["status"],
        "success"
    );
    assert_eq!(
        state_value["facts"][0]["payload"]["current"]["conclusion"],
        "success"
    );
    stop(&layout, handle).await;
}
