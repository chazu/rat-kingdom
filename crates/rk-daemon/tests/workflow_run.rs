//! Phase 5 end to end: a CUE-defined workflow (spawn → wait → evaluate →
//! dismiss, with an aspect and per-node agent profiles) runs against the fake
//! harness, and the runner resolves harness/model through the layered agent
//! config.

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

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work for $RK_TASK by $RK_AGENT (model: $RK_MODEL_MARKER)" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const WORKFLOW: &str = r#"
workflow: {
    name: "build-and-check"
    params: {
        taskId: {type: "string", required: true}
    }
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "Do the thing for " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
    aspects: [
        {match: {type: "dismiss"}, before: [{type: "gate", gateType: "timer", duration: "1s"}]},
    ]
}
"#;

#[tokio::test]
async fn cue_workflow_runs_end_to_end_with_agent_resolution() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    // Definition discovered from the repo-local workflows dir.
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("build-and-check.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    std::env::set_var("RK_MODEL_MARKER", "unset");
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let defs = client
        .call(
            "workflow.definitions",
            json!({"repo": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    assert!(defs["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d == "build-and-check"));

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "build-and-check",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "wf-task-1"},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();
    // Aspect added the timer gate: 5 steps total.
    assert_eq!(started["instance"]["total_steps"], 5);

    let mut completed = false;
    for _ in 0..200 {
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
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    assert!(completed, "workflow did not complete");

    // The spawned rat resolved through agents.default (harness fake, model
    // sonnet — recorded on the agent).
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let agent = &agents["agents"][0];
    assert_eq!(agent["harness"], "fake");
    assert_eq!(agent["model"], "sonnet");
    assert_eq!(agent["state"], "dismissed");

    // The dismiss step merged the rat's work into main.
    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&files.stdout).to_string();
    assert!(
        listing.contains("work-"),
        "merged work file in main: {listing}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
