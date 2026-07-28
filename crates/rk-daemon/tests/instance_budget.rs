//! TKT-32: per-workflow-instance budget caps. A workflow's `budget:` field
//! caps the SUM of its own spawned agents' cost. Once that instance's spend
//! reaches the cap, further dispatch (here, a later `spawn` step) is refused —
//! the wallet kill-switch scoped to one workflow run, layered below the global
//! fleet/repo caps. Fleet/repo caps stay unlimited so only the instance cap can
//! bite.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::time::Duration;

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Completes immediately, self-reporting a $0.50 authoritative cost — one such
/// rat alone puts its instance over a $0.30 cap.
const SPENDER_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"spender-1"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"spender-1","total_cost_usd":0.5,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// spawn → wait → spawn. After the first rat completes (and its $0.50 cost is
// recorded, over the $0.30 instance cap), the `wait` returns, so the second
// spawn's dispatch preflight sees the instance over budget and refuses it.
const WORKFLOW: &str = r#"
workflow: {
    name: "instance-budget-test"
    params: {repo: {type: "string", required: false, default: ""}}
    budget: {max_usd: 0.30}
    agents: {default: {harness: "fake", model: "haiku"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: "burn-1", description: "spend"}},
        {type: "wait", timeout: "60s"},
        {type: "spawn", role: "rat", task: {title: "burn-2", description: "spend again"}},
    ]
}
"#;

#[tokio::test]
async fn instance_cap_refuses_later_dispatch_once_hit() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("instance-budget-test.cue"), WORKFLOW).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", SPENDER_FAKE);
    let layout = Layout::at(home.path());
    // Fleet/repo caps unlimited (new_in_memory uses default BudgetConfig): only
    // the per-instance cap can refuse a spawn here.
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "instance-budget-test",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    // The instance must FAIL: the second spawn is refused once the first rat's
    // $0.50 spend crosses the $0.30 instance cap.
    let mut failed = false;
    let mut err = String::new();
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "failed" => {
                err = status["instance"]["error"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                failed = true;
                break;
            }
            "completed" => panic!("workflow completed but the instance cap should have refused the second spawn"),
            _ => {}
        }
    }
    assert!(failed, "workflow instance did not fail on the instance cap");
    assert!(
        err.contains("instance budget cap hit") || err.contains("dispatch refused"),
        "failure names the instance cap: {err}"
    );

    // Only the first rat was ever dispatched; the second (burn-2) was refused
    // before any worktree/record existed.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let tasks: Vec<String> = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["task"].as_str().map(String::from))
        .collect();
    assert!(tasks.contains(&"burn-1".to_string()), "first rat spawned");
    assert!(
        !tasks.contains(&"burn-2".to_string()),
        "second rat must never have been dispatched: {tasks:?}"
    );

    // The refusal surfaced an instance-scoped obstacle naming the instance.
    let obstacles = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    let named = obstacles["tuples"].as_array().unwrap().iter().any(|t| {
        t["payload"]["type"].as_str() == Some("budget_instance_exceeded")
            && t["payload"]["instance"].as_str() == Some(id.as_str())
    });
    assert!(named, "instance-exceeded obstacle names the instance");

    // `rk cost --fleet` rollup attributes the spend to this instance.
    let rollup = client.call("budget.rollup", json!({})).await.unwrap();
    let inst_spend = rollup["instances"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["instance"].as_str() == Some(id.as_str()))
        .and_then(|r| r["spent_usd"].as_f64())
        .unwrap_or(0.0);
    assert!(
        inst_spend >= 0.30,
        "instance rollup reflects the spend: {inst_spend}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
