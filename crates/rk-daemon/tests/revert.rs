//! TKT-54: `rk revert <agent>` — the operator undo for a bad unattended
//! auto-merge. A dismissed rat's merge commit is recorded on its registry
//! record; `agent.revert` revert-merges it on the target, reopens the rat's
//! ticket (`open`, or `blocked` with `block`), and emits a `fact` tuple.
//! The anchor is cleared on success so a second revert errors instead of
//! reverting the revert.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
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

fn git_out(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_passing_landing_checks(dir);
}

/// Fake harness: commits a file in its worktree, reports a clean success.
/// Declares `rk done` before its result line: a clean turn that never does
/// now parks the agent as `Paused` (awaiting resume) rather than `Completed`,
/// which every test here waits on.
///
/// `RK_FAKE_HARNESS_CMD` is process-global, and this binary's two tests run
/// concurrently, so neither test may ever `remove_var` it: doing so at the
/// end of one test can unset the fake mid-flight for the other test's still-
/// spawning agent, which then falls back to a different default script and
/// never reaches the state either test is waiting on (TKT-88 — mirrors the
/// same precaution in fleet_budget.rs/merge_queue.rs/pr_mode.rs). Both tests
/// set the identical value, so leaving it set for the whole process is
/// harmless.
fn working_fake() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
echo "bad work by $RK_AGENT for $RK_TASK" > regression.txt
git add regression.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"revert-fake"}'
rk_done "done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"revert-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#,
    )
}

/// Spawn a ticket-dispatched rat, wait for completion, dismiss (auto-merge).
/// Returns (agent name, ticket id).
async fn merge_one_rat(client: &mut Client, repo: &Path) -> (String, String) {
    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "do the thing", "scope": "svc"}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo.to_string_lossy(),
                "task": ticket_id,
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": &name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "rat {name} never completed");

    let dismissed = client
        .call("agent.dismiss", json!({"name": &name}))
        .await
        .unwrap();
    assert_eq!(
        dismissed["merged"], false,
        "detail: {}",
        dismissed["detail"]
    );
    let landed = client
        .call(
            "repo.land",
            json!({"repo": repo, "branch": branch, "target": "main"}),
        )
        .await
        .unwrap();
    assert_eq!(landed["merged"], true, "detail: {}", landed["detail"]);
    assert!(
        landed["merge_commit"]
            .as_str()
            .is_some_and(|c| !c.is_empty()),
        "gated land records the merge commit"
    );
    (name, ticket_id)
}

#[tokio::test]
async fn revert_undoes_merge_reopens_ticket_and_emits_fact() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", working_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let (name, ticket_id) = merge_one_rat(&mut client, repo_dir.path()).await;
    assert!(repo_dir.path().join("regression.txt").exists());
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "closed");

    // The undo: revert-merge the landed commit.
    let reverted = client
        .call("agent.revert", json!({"name": &name}))
        .await
        .unwrap();
    assert_eq!(reverted["reverted"], true, "detail: {}", reverted["detail"]);
    assert!(
        reverted["revert_commit"]
            .as_str()
            .is_some_and(|c| !c.is_empty()),
        "revert reports the revert commit"
    );

    // The bad work is gone from main's tree AND the root checkout; history
    // keeps both the merge and the revert.
    let files = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        !files.contains("regression.txt"),
        "main tree still has the bad file"
    );
    assert!(!repo_dir.path().join("regression.txt").exists());
    let log = git_out(repo_dir.path(), &["log", "--oneline", "main"]);
    assert!(log.contains("Revert"));

    // The ticket the bad merge closed is back on the backlog.
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "open");

    // The revert left a durable fact tuple behind.
    let facts = client
        .call(
            "space.scan",
            json!({"category": "fact", "identity": format!("merge-reverted-{name}")}),
        )
        .await
        .unwrap();
    let fact = &facts["tuples"][0];
    assert_eq!(fact["payload"]["agent"], name.as_str());
    assert_eq!(fact["payload"]["task"], ticket_id.as_str());
    assert_eq!(fact["payload"]["ticket_status"], "open");
    assert!(fact["payload"]["revert_commit"].as_str().is_some());

    // The anchor is cleared: a second revert errors rather than reverting
    // the revert.
    let again = client.call("agent.revert", json!({"name": &name})).await;
    assert!(again.is_err(), "second revert must error");
}

#[tokio::test]
async fn revert_block_reopens_ticket_blocked_and_never_merged_errors() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", working_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let (name, ticket_id) = merge_one_rat(&mut client, repo_dir.path()).await;

    // --block holds the reopened ticket out of the auto-dispatch backlog.
    let reverted = client
        .call("agent.revert", json!({"name": &name, "block": true}))
        .await
        .unwrap();
    assert_eq!(reverted["reverted"], true, "detail: {}", reverted["detail"]);
    assert_eq!(reverted["ticket_status"], "blocked");
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "blocked");

    // A rat dismissed WITHOUT a merge has no anchor: revert errors.
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "held-work",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let held = spawned["agent"]["name"].as_str().unwrap().to_string();
    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": &held}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "rat {held} never completed");
    let dismissed = client
        .call("agent.dismiss", json!({"name": &held, "no_merge": true}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], false);
    let denied = client.call("agent.revert", json!({"name": &held})).await;
    assert!(denied.is_err(), "revert of a never-merged agent must error");
}
