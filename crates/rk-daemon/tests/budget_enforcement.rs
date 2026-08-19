//! Phase 4: budget enforcement — a runaway (fake) rat crosses its cost cap
//! and is warned, then killed, with obstacle tuples for the coordinator.

mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use rk_ledger::Budget;
use rk_space::Space;
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

/// A runaway fake: emits growing usage forever (haiku pricing: 1e-6/in,
/// 5e-6/out — each burst of 200k in + 40k out ≈ $0.40) and never completes.
const RUNAWAY_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"runaway-1"}'
for i in $(seq 1 50); do
  echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"burning tokens"}],"usage":{"input_tokens":200000,"output_tokens":40000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
  sleep 0.2
done
"#;

#[tokio::test]
async fn runaway_rat_is_warned_then_stopped_at_the_cap() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("f"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", RUNAWAY_FAKE);
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

    // Model set so the pricing table applies (fake reports no cost itself
    // on usage events).
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "runaway-1",
                "harness": "fake",
                "model": "haiku",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for the budget stop: the agent ends up failed (killed without
    // completing) with cost >= the cap.
    let mut stopped = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        let state = status["agent"]["state"].as_str().unwrap_or("");
        if state == "failed" {
            assert!(
                status["agent"]["cost_usd"].as_f64().unwrap() >= 1.0,
                "stopped at/after the cap"
            );
            stopped = true;
            break;
        }
    }
    assert!(stopped, "runaway rat was not stopped");

    // Both graduated steps left obstacle tuples.
    let obstacles = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    let kinds: Vec<String> = obstacles["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["payload"]["type"].as_str().unwrap_or("").to_string())
        .collect();
    assert!(
        kinds.contains(&"budget_warning".to_string()),
        "warning fired: {kinds:?}"
    );
    assert!(
        kinds.contains(&"budget_exceeded".to_string()),
        "stop fired: {kinds:?}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
