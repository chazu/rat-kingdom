//! The operator-facing half of TKT-01M0HNDJ7AS9F1A3W22FRCC63N, end to end
//! over the socket.
//!
//! `Supervisor::continue_recovery`/`abandon_recovery` and the durable
//! `RecoveryRecord` are covered by focused unit tests in `supervisor.rs`.
//! What those cannot reach is the seam this file exists for: a parked
//! generation must be DISCOVERABLE and ACTIONABLE without hand-editing
//! `agents.json` — an `rk inbox` row an operator actually sees, and an RPC
//! method (behind `rk continue-recovery` / `rk abandon-recovery`) that carries
//! the at-most-once `action_id` contract across the wire rather than only
//! in-process.
//!
//! Abandonment is the continuation shape exercised here on purpose: it drives
//! the identical params/authorization/ack path as `agent.continue_recovery`
//! while launching no harness, so the wire contract is proven without this
//! test also owning a second live-process lifecycle.

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;
use support::connect;

fn git(dir: &Path, args: &[&str]) {
    let out = std::process::Command::new("git")
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

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

/// A rat that COMMITS work in its worktree and only then loses its transport.
/// Both halves matter: `detect_post_commit_outage` is a no-op unless the
/// branch has a commit past its fork point (that is what separates this from
/// an ordinary crashed launch) AND the stderr tail classifies as a transport
/// signal. It never reports a result, so the daemon sees a silent nonzero
/// death carrying that stderr.
const FAKE: &str = r#"
read -r _prompt
git config user.email rat@example.com
git config user.name Rat
echo 'work' > delivered.txt
git add delivered.txt
git commit -q -m 'committed work before the outage'
echo 'fatal: connection refused while contacting api' >&2
exit 1
"#;

#[tokio::test]
async fn a_parked_post_commit_recovery_is_visible_in_the_inbox_and_actionable_over_rpc() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "outage-1",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for detection rather than for a fixed sleep: the commit, the
    // child's death, the `Exited` event and the recovery write are four
    // independent hops.
    let mut recovery = Value::Null;
    for _ in 0..100 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if !status["agent"]["recovery"].is_null() {
            recovery = status["agent"]["recovery"].clone();
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !recovery.is_null(),
        "a post-commit transport outage must park a durable recovery record"
    );
    assert!(recovery["ack"].is_null(), "nothing has acted on it yet");
    assert!(!recovery["head"].as_str().unwrap().is_empty());

    // (2) The reviewer's second ask: it must surface the way every other
    // automated recovery source does, so an operator sees it without polling
    // raw per-agent status JSON.
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    let row = inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|it| it["kind"] == "recovery-action" && it["subject"] == name.as_str())
        .cloned()
        .expect("a parked recovery must raise a recovery-action inbox row");
    let detail = row["detail"].as_str().unwrap();
    assert!(
        detail.contains(&format!("rk continue-recovery {name}")),
        "the row must name the remedy, since its own action is always `rk inbox ack`: {detail}"
    );
    assert!(
        detail.contains(&format!("rk abandon-recovery {name}")),
        "{detail}"
    );

    // (1) The reviewer's first ask: reachable over RPC, not just as a `pub fn`
    // with no caller.
    let abandoned = client
        .call(
            "agent.abandon_recovery",
            json!({"name": name, "action_id": "op-1"}),
        )
        .await
        .unwrap();
    assert_eq!(abandoned["outcome"], "Abandoned");

    // At-most-once, across the wire: the SAME key replays the recorded
    // outcome instead of acting twice...
    let replay = client
        .call(
            "agent.abandon_recovery",
            json!({"name": name, "action_id": "op-1"}),
        )
        .await
        .unwrap();
    assert_eq!(
        replay["outcome"], "Abandoned",
        "replaying an action_id must return the recorded outcome, not re-act"
    );

    // ...and a DIFFERENT key after acknowledgement is refused, so a second
    // operator cannot continue work another already wrote off.
    let conflict = client
        .call(
            "agent.continue_recovery",
            json!({"name": name, "action_id": "op-2", "harness": Value::Null}),
        )
        .await;
    assert!(
        conflict.is_err(),
        "a different action_id after acknowledgement must be refused: {conflict:?}"
    );
}
