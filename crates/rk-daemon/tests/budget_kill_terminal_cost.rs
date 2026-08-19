//! Regression for the terminal-cost-under-report bug: a budget-killed agent
//! whose harness still flushes a `Completed` event after the SIGTERM (with a
//! self-reported cost/usage reflecting only the partial final turn) must not
//! have that lower figure clobber the true, budget-machinery-computed spend
//! in its terminal `agents.json` record. The `budget_exceeded` obstacle
//! always carried the true number; the archived agent record did not
//! (observed live: Wensleydale-7 recorded $3.85 vs $20.04 actual).

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
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
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// A runaway fake (haiku pricing: 1e-6/in, 5e-6/out — each burst of 200k in +
/// 40k out ≈ $0.40) that, when SIGTERM'd (the budget hard-stop), still
/// flushes one more `result` line the way a real harness winding down
/// mid-turn does — but with a `total_cost_usd` far below what was actually
/// spent (the partial final turn only, not the cumulative session). Without
/// a floor, that low figure is what lands in the terminal record.
const RUNAWAY_THEN_UNDERREPORTS_ON_KILL: &str = r#"
trap 'echo "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"stopped\",\"session_id\":\"budget-kill-1\",\"total_cost_usd\":0.05,\"usage\":{\"input_tokens\":100,\"output_tokens\":50,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0}}"; exit 0' TERM
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"budget-kill-1"}'
for i in $(seq 1 50); do
  echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"burning tokens"}],"usage":{"input_tokens":200000,"output_tokens":40000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
  sleep 0.2
done
"#;

#[tokio::test]
async fn budget_killed_terminal_record_keeps_the_true_cost() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("f"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", RUNAWAY_THEN_UNDERREPORTS_ON_KILL);
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget {
            max_usd: 1.0,
            max_tokens: 0,
            warn_at: 0.5,
        },
        space,
    )
    .unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "budget-kill-1",
                "harness": "fake",
                "model": "haiku",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for the agent to leave the live states (Spawning/Running).
    let mut terminal_cost = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        let state = status["agent"]["state"].as_str().unwrap_or("");
        if state != "spawning" && state != "running" {
            terminal_cost = status["agent"]["cost_usd"].as_f64();
            break;
        }
    }
    let terminal_cost = terminal_cost.expect("budget-killed rat reached a terminal state");

    // The obstacle carries the true, budget-machinery-computed figure.
    let obstacles = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    let exceeded_cost = obstacles["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["payload"]["type"] == "budget_exceeded")
        .and_then(|t| t["payload"]["cost_usd"].as_f64())
        .expect("budget_exceeded obstacle carries cost_usd");

    assert!(
        exceeded_cost >= 1.0,
        "obstacle recorded the true cap-crossing cost: {exceeded_cost}"
    );
    // The terminal record must match the obstacle's true figure, not the
    // harness's post-kill self-report of $0.05.
    assert!(
        (terminal_cost - exceeded_cost).abs() < 0.01,
        "terminal record ({terminal_cost}) must match the obstacle's true cost \
         ({exceeded_cost}), not the harness's post-kill under-report"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
