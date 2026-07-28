//! Phase 2 exit criteria, end to end over the socket: spawn a (fake-harness)
//! rat into a real worktree, watch it complete, verify parent routing, dismiss
//! with merge, and confirm main received the work.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Fake harness script: does real git work in its cwd (the worktree), then
/// emits a Claude-style completion.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "gnawed by $RK_AGENT for task $RK_TASK" > gnawed.txt
git add gnawed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"fake-e2e"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"committed gnawed.txt","session_id":"fake-e2e","total_cost_usd":0.002,"usage":{"input_tokens":50,"output_tokens":25,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test]
async fn spawn_complete_route_dismiss_merge() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Spawn with a parent so completion routing is exercised.
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "gnaw-1",
                "harness": "fake",
                "parent": "KingRat",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    assert_eq!(spawned["agent"]["state"], "running");
    assert_eq!(spawned["agent"]["parent"], "KingRat");

    // Wait for completion (structural: registry state, no sleep-polling the pane).
    let mut completed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            assert_eq!(status["agent"]["result"], "committed gnawed.txt");
            assert_eq!(status["agent"]["cost_usd"], 0.002);
            assert_eq!(status["agent"]["session_id"], "fake-e2e");
            completed = true;
            break;
        }
    }
    assert!(completed, "agent never completed");

    // Completion was routed: repo event + directed message to the parent.
    let events = client
        .call(
            "space.scan",
            json!({"category": "event", "identity": "harness_result"}),
        )
        .await
        .unwrap();
    assert_eq!(events["tuples"].as_array().unwrap().len(), 1);
    let parent_msg = client
        .call(
            "space.scan",
            json!({"category": "message", "identity": "KingRat"}),
        )
        .await
        .unwrap();
    assert_eq!(parent_msg["tuples"][0]["payload"]["child"], name);
    assert_eq!(parent_msg["tuples"][0]["payload"]["is_error"], false);

    // Dismiss merges the rat's branch into main.
    let dismissed = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], true, "detail: {}", dismissed["detail"]);

    let files = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(files.contains("gnawed.txt"), "main has the rat's work");
    let log = git_out(repo_dir.path(), &["log", "--oneline", "main"]);
    assert!(log.contains("rat work: gnaw-1"));

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A rat dispatched with a ticket id as its task closes the ticket's loop:
/// completion → `done`, merge-on-dismiss → `closed`.
#[tokio::test]
async fn ticket_dispatched_rat_closes_its_ticket() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // File a ticket, then dispatch a rat whose task IS that ticket id.
    let ticket = client
        .call("ticket.new", json!({"title": "do the thing", "scope": "svc"}))
        .await
        .unwrap();
    let id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": id,
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // On completion the ticket becomes `done`.
    let mut done = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let t = client.call("ticket.get", json!({"id": id})).await.unwrap();
        if t["ticket"]["payload"]["status"] == "done" {
            done = true;
            break;
        }
    }
    assert!(done, "ticket was not marked done on completion");

    // Dismiss with merge closes it for good.
    let dismissed = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], true);
    let t = client.call("ticket.get", json!({"id": id})).await.unwrap();
    assert_eq!(t["ticket"]["payload"]["status"], "closed");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
