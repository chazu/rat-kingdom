//! TKT-57 end to end: a `sub_workflow` step runs another named workflow inline,
//! to completion, and joins the child's result into the parent's
//! ctx.previousResult — so a parent `evaluate` can gate on how the child
//! finished. A second test proves the nesting depth cap fails a workflow cycle
//! closed instead of recursing without end.

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work for $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// The child: a plain build-and-merge. Its final step is `dismiss`, so its
// ctx.previousResult ends as the dismiss outcome ({merged: true, ...}) — that is
// the value the parent joins on.
const CHILD: &str = r#"
workflow: {
    name: "child-build"
    params: {taskId: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

// The parent: run the child by name (forwarding a templated param), then gate on
// the child's joined result — {merged: true} proves the child ran to completion
// AND its dismiss outcome reached the parent's ctx.previousResult.
const PARENT: &str = r#"
workflow: {
    name: "parent-compose"
    params: {taskId: {type: "string", required: true}}
    steps: [
        {type: "sub_workflow", workflow: "child-build", params: {taskId: _input.taskId}},
        {type: "evaluate", expect: {merged: true}},
    ]
}
"#;

#[tokio::test]
async fn sub_workflow_runs_child_and_joins_its_result() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("child-build.cue"), CHILD).unwrap();
    std::fs::write(wf_dir.join("parent-compose.cue"), PARENT).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "parent-compose",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "compose-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            "failed" => panic!("parent workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "parent workflow did not complete");

    // The child's dismiss merged its rat's work into main — proof the parent's
    // sub_workflow step drove the child all the way through its own dismiss.
    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(
        listing.contains("work-"),
        "child work must merge into main: {listing}"
    );

    // The child instance is recorded independently (own id, completed), so it
    // shows up in `rk workflow list` just like a top-level run.
    let list = client.call("workflow.list", json!({})).await.unwrap();
    let instances = list["instances"].as_array().unwrap();
    let child = instances
        .iter()
        .find(|i| i["workflow"] == "child-build")
        .expect("child instance recorded");
    assert_eq!(child["status"], "completed");
    assert_ne!(
        child["id"].as_str().unwrap(),
        id,
        "child has its own instance id"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// A workflow whose only step runs itself — an unbounded cycle but for the depth
// cap that fails it closed.
const CYCLE: &str = r#"
workflow: {
    name: "cycle"
    steps: [
        {type: "sub_workflow", workflow: "cycle"},
    ]
}
"#;

#[tokio::test]
async fn sub_workflow_cycle_fails_closed_at_depth_cap() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("cycle.cue"), CYCLE).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "cycle",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut failed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "failed" => {
                let err = status["instance"]["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("nesting too deep"),
                    "expected a depth-cap failure, got: {err}"
                );
                failed = true;
                break;
            }
            "completed" => panic!("a workflow cycle must not complete"),
            _ => {}
        }
    }
    assert!(failed, "workflow cycle did not fail closed at the depth cap");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
