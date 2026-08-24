//! Gap 1 of TKT-01M0HNE2FYHYS5HCDW618VRQJD: `post_commit_recovery_rpc.rs`
//! proves the wire and durability for the generic fake harness, but the
//! alternate-harness-continuation and daemon-restart-mid-recovery seams were
//! only ever exercised in-process against the pure `Supervisor` (see
//! `continue_recovery_routes_to_a_configured_alternate_harness` and
//! `abandoned_recovery_stays_excluded_from_respawn_sweep_forever` in
//! `supervisor.rs`). This file proves the missing wire/restart boundary on
//! real `RK_CLAUDE_BIN`/`RK_CODEX_BIN` adapters, mirroring the kill/replace
//! daemon pattern from `transport_outage.rs`. It does not re-derive the
//! outcome logic those unit tests already cover.

mod support;

use rk_core::config::SupervisorConfig;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// Both tests in this file set the process-global `RK_CLAUDE_BIN`/
/// `RK_CODEX_BIN` env vars (mirrors `transport_outage.rs`'s
/// `HARNESS_ENV_LOCK`) — without this, cargo's default concurrent test
/// execution lets one test's fixture path clobber the other's mid-run.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# recovery continuation fixture\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

fn sweep_config() -> SupervisorConfig {
    SupervisorConfig {
        enabled: true,
        interval_secs: 1,
        stuck_after_secs: 0,
        burn_usd_per_min: 0.0,
        kill_grace_secs: 1,
        respawn_enabled: false,
        ..SupervisorConfig::default()
    }
}

/// Unlike `sweep_config`, auto-respawn is ON with no backoff — the restarted
/// daemon's own background sweep loop gets real, repeated chances to
/// resurrect a Failed record within the test's short sleep window. That is
/// the point: proving an abandoned recovery's exclusion holds under a live
/// sweep loop across a restart, not only when a test calls
/// `Supervisor::respawn_sweep` by hand in-process.
fn respawn_sweep_config() -> SupervisorConfig {
    SupervisorConfig {
        enabled: true,
        interval_secs: 1,
        stuck_after_secs: 0,
        burn_usd_per_min: 0.0,
        kill_grace_secs: 1,
        respawn_enabled: true,
        respawn_max_attempts: 3,
        respawn_backoff_secs: 0,
        ..SupervisorConfig::default()
    }
}

fn daemon(layout: &Layout) -> Daemon {
    let space = Space::open(&layout.db_path()).unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "claude".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_sweep_config(sweep_config());
    daemon
}

fn daemon_with_respawn(layout: &Layout) -> Daemon {
    let space = Space::open(&layout.db_path()).unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "claude".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_sweep_config(respawn_sweep_config());
    daemon
}

fn marker_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().count())
        .unwrap_or(0)
}

async fn wait_for_status(
    client: &mut Client,
    name: &str,
    predicate: impl Fn(&Value) -> bool,
    description: &str,
) -> Value {
    for _ in 0..600 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if predicate(&status) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {description}");
}

/// A real Claude adapter child that reaches its `Started` handshake (so the
/// pre-work transport watcher never fires — `detect_post_commit_outage` would
/// otherwise be shadowed by the pre-work `TransportFailure` path, see
/// `rk_harness::watch_pre_work_transport_failure`), commits work in its
/// worktree, then dies on a transport-classified stderr signal.
const CLAUDE_POST_COMMIT_OUTAGE: &str = r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"pre-outage-session"}'
git config user.email rat@example.com
git config user.name Rat
echo work > delivered.txt
git add delivered.txt
git commit -q -m 'committed work before the outage'
printf '%s\n' 'fatal: connection refused while contacting api' >&2
exit 1
"#;

/// A real Codex adapter child that reaches its own `Started` handshake
/// (`thread.started`) and exits cleanly — the alternate-harness continuation
/// target.
const CODEX_CONTINUATION: &str = r#"#!/bin/sh
printf '%s\n' '{"type":"thread.started","thread_id":"continued-session"}'
exit 0
"#;

/// Same shape as [`CLAUDE_POST_COMMIT_OUTAGE`], but counts its own
/// invocations to a marker file so a test can prove an abandoned generation
/// is never relaunched — across a real, restarted daemon's live sweep loop,
/// not just a single in-process `respawn_sweep` call.
fn claude_post_commit_outage_with_marker(marker: &Path) -> String {
    format!(
        r#"#!/bin/sh
printf 'attempt\n' >> '{}'
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"pre-outage-session"}}'
git config user.email rat@example.com
git config user.name Rat
echo work > delivered.txt
git add delivered.txt
git commit -q -m 'committed work before the outage'
printf '%s\n' 'fatal: connection refused while contacting api' >&2
exit 1
"#,
        marker.display()
    )
}

/// Checkpoint scenario for gap 1(a)/(c): a post-commit recovery parked by a
/// real Claude adapter is continued under a real, configured alternate
/// harness (Codex) over RPC, and the parked record — plus the at-most-once
/// `action_id` ack it eventually carries — survives a daemon restart
/// injected between park and continuation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_recovery_routes_to_a_real_alternate_harness_across_a_daemon_restart() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let claude = home.path().join("claude-fixture");
    executable(&claude, CLAUDE_POST_COMMIT_OUTAGE);
    let codex = home.path().join("codex-fixture");
    executable(&codex, CODEX_CONTINUATION);
    std::env::set_var("RK_CLAUDE_BIN", &claude);
    std::env::set_var("RK_CODEX_BIN", &codex);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let handle_a = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "post-commit-outage",
                "harness": "claude",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let original_spawn = spawned["agent"]["spawn"].clone();

    let parked = wait_for_status(
        &mut client,
        &name,
        |s| !s["agent"]["recovery"].is_null(),
        "a real post-commit outage to park a durable recovery record",
    )
    .await;
    assert_eq!(
        parked["agent"]["recovery"]["ack"],
        Value::Null,
        "nothing has acted on it yet"
    );
    assert_eq!(parked["agent"]["recovery"]["provider"], "claude");

    // Restart the daemon BETWEEN detection and continuation: the record this
    // relies on must be a durable seam, not an in-memory one owned by the
    // detecting process (mirrors transport_outage.rs's kill/replace pattern).
    client.call("stop", json!({})).await.unwrap();
    handle_a.await.unwrap().unwrap();
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let handle_b = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;

    let response = client
        .call(
            "agent.continue_recovery",
            json!({"name": name, "action_id": "alt-1", "harness": "codex"}),
        )
        .await
        .unwrap();
    let outcome = response["outcome"].clone();
    assert_eq!(outcome["ContinuedAlternateProvider"]["harness"], "codex");
    assert_eq!(
        outcome["ContinuedAlternateProvider"]["new_spawn"], original_spawn,
        "continuation must preserve the original generation's identity"
    );

    // At-most-once across the restart boundary: replaying the SAME
    // action_id must return the recorded outcome, not launch a second real
    // Codex process.
    let replay = client
        .call(
            "agent.continue_recovery",
            json!({"name": name, "action_id": "alt-1", "harness": "codex"}),
        )
        .await
        .unwrap();
    assert_eq!(
        replay["outcome"], outcome,
        "replaying the action_id must return the recorded outcome, not re-act"
    );

    let settled = wait_for_status(
        &mut client,
        &name,
        |s| {
            !matches!(
                s["agent"]["state"].as_str(),
                Some("spawning" | "running" | "paused")
            )
        },
        "the real Codex continuation to settle",
    )
    .await;
    assert_eq!(
        settled["agent"]["harness"], "codex",
        "the record must reflect the alternate harness it actually continued under"
    );

    client.call("stop", json!({})).await.unwrap();
    handle_b.await.unwrap().unwrap();
    std::env::remove_var("RK_CLAUDE_BIN");
    std::env::remove_var("RK_CODEX_BIN");
}

/// Gap 1(b): `abandoned_recovery_stays_excluded_from_respawn_sweep_forever`
/// (supervisor.rs) proves the exclusion by calling `respawn_sweep` once,
/// in-process, against a hand-built record. This proves the same terminal
/// outcome — WIP release stays permanent, the harness never relaunches — over
/// RPC on a real Claude adapter, under a RESTARTED daemon whose own
/// background sweep loop is live and auto-respawn-enabled, so it gets
/// several real, unmocked chances to resurrect the generation and must not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_recovery_stays_terminal_across_a_restart_under_a_live_respawn_sweep() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let marker = home.path().join("claude-attempts");
    let claude = home.path().join("claude-fixture");
    executable(&claude, &claude_post_commit_outage_with_marker(&marker));
    std::env::set_var("RK_CLAUDE_BIN", &claude);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let handle_a = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "post-commit-outage-abandon",
                "harness": "claude",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    wait_for_status(
        &mut client,
        &name,
        |s| !s["agent"]["recovery"].is_null(),
        "a real post-commit outage to park a durable recovery record",
    )
    .await;
    assert_eq!(marker_count(&marker), 1);

    let abandoned = client
        .call(
            "agent.abandon_recovery",
            json!({"name": name, "action_id": "give-up-1"}),
        )
        .await
        .unwrap();
    assert_eq!(abandoned["outcome"], "Abandoned");

    // Restart into a daemon whose sweep config actually auto-respawns Failed
    // agents (unlike `daemon()`'s config, used above only to detect the
    // outage without any respawn interference).
    client.call("stop", json!({})).await.unwrap();
    handle_a.await.unwrap().unwrap();
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let handle_b = tokio::spawn(daemon_with_respawn(&layout).run());
    let mut client = connect(&layout).await;

    // Cross several real sweep ticks (1s interval, 0 backoff) and prove none
    // of them resurrect it.
    tokio::time::sleep(Duration::from_millis(3_500)).await;
    let after = client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(
        after["agent"]["state"], "failed",
        "an abandoned recovery must never be auto-respawned, even under a live sweep loop \
         after a restart"
    );
    assert!(
        after["agent"]["pid"].is_null(),
        "WIP must stay released permanently"
    );
    assert_eq!(after["agent"]["recovery"]["ack"]["outcome"], "Abandoned");
    assert_eq!(
        marker_count(&marker),
        1,
        "the abandoned generation must never relaunch its harness"
    );

    client.call("stop", json!({})).await.unwrap();
    handle_b.await.unwrap().unwrap();
    std::env::remove_var("RK_CLAUDE_BIN");
}
