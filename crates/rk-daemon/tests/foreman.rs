//! End-to-end foreman flow: a workflow spawns a foreman, the foreman spawns a
//! worker, receives the daemon-routed completion message, dismisses the worker
//! into its integration branch, and the workflow lands that branch on main.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
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

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

const WORKFLOW: &str = r#"
workflow: {
    name: "foreman-test"
    params: {
        taskId: {type: "string", required: true}
    }
    agents: {default: {harness: "fake"}}
    budget: {max_usd: 5}
    steps: [
        {type: "spawn", role: "foreman", task: {title: "foreman-test"}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

#[tokio::test]
async fn foreman_delegates_merges_and_lands_worker() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    let rk = std::env::var("CARGO_BIN_EXE_rk").unwrap_or_else(|_| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("debug")
            .join("rk")
            .to_string_lossy()
            .into_owned()
    });
    let script = format!(
        r#"
read -r _prompt
if [ "$RK_ROLE" = "foreman" ]; then
  child_json=$('{rk}' --json spawn --task child-task --repo "$RK_WORKTREE" \
    --prompt "Implement the child task, commit the change, then run rk done" \
    --parent "$RK_AGENT" --base "$RK_BRANCH")
  child=$(printf '%s' "$child_json" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')
  test -n "$child"
  '{rk}' rd message "$RK_REPO" "$RK_AGENT" --timeout 30s >/dev/null
  '{rk}' dismiss "$child" >/dev/null
  echo '{{"type":"system","subtype":"init","session_id":"foreman-fake"}}'
  rk_done "integrated child worker"
  echo '{{"type":"result","subtype":"success","is_error":false,"result":"integrated child worker","session_id":"foreman-fake","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
else
  echo "worker output" > child.txt
  git add child.txt
  git -c user.email=rat@x -c user.name=Rat commit -q -m "worker: child task"
  echo '{{"type":"system","subtype":"init","session_id":"worker-fake"}}'
  rk_done "implemented child task"
  echo '{{"type":"result","subtype":"success","is_error":false,"result":"implemented child task","session_id":"worker-fake","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
fi
"#,
        rk = rk
    );

    let workflow_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&workflow_dir).unwrap();
    std::fs::write(workflow_dir.join("foreman-test.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(&script));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "foreman-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {"taskId": "feature-1"},
            }),
        )
        .await
        .unwrap();
    let instance = started["instance"]["id"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": instance}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            "failed" => panic!("foreman workflow failed: {status}"),
            _ => {}
        }
    }
    assert!(completed, "foreman workflow did not complete");

    let files = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["ls-tree", "--name-only", "main"])
        .output()
        .unwrap();
    let files = String::from_utf8_lossy(&files.stdout);
    assert!(files.lines().any(|line| line == "child.txt"));

    let agents = client
        .call("agent.list", json!({"include_archived": true}))
        .await
        .unwrap();
    let rows = agents["agents"].as_array().unwrap();
    let foreman = rows
        .iter()
        .find(|row| row["role"] == "foreman")
        .expect("foreman record");
    let worker = rows
        .iter()
        .find(|row| row["task"] == "child-task")
        .unwrap_or_else(|| panic!("worker record not found in {agents}"));
    assert_eq!(worker["parent"], foreman["name"]);
    assert_eq!(worker["workflow_instance"], instance);

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
