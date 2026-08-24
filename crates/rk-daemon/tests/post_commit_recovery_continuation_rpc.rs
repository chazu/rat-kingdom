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

/// Checkpoint scenario for gap 1(a)/(c): a post-commit recovery parked by a
/// real Claude adapter is continued under a real, configured alternate
/// harness (Codex) over RPC, and the parked record — plus the at-most-once
/// `action_id` ack it eventually carries — survives a daemon restart
/// injected between park and continuation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_recovery_routes_to_a_real_alternate_harness_across_a_daemon_restart() {
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
