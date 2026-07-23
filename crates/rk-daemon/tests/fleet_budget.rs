//! TKT-16: hierarchical fleet/repo budget caps. Once the fleet-wide cost sum
//! reaches its cap, new spawns are refused (the wallet kill-switch) — even
//! though each individual agent stayed under its own per-agent budget.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::{Budget, FleetBudget};
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
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Completes immediately, self-reporting a $0.50 authoritative cost — one such
/// rat alone puts the fleet over a $0.30 cap.
const SPENDER_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"spender-1"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"spender-1","total_cost_usd":0.5,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test]
async fn fleet_cap_refuses_dispatch_once_hit() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("f"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", SPENDER_FAKE);
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let daemon = Daemon::with_fleet_budget_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        // Generous per-agent cap so only the fleet cap can bite.
        Budget {
            max_usd: 100.0,
            max_tokens: 0,
            warn_at: 0.8,
        },
        FleetBudget {
            fleet_max_usd: 0.30,
            repo_max_usd: 0.0,
            warn_at: 0.8,
        },
        space,
    )
    .unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let repo = repo_dir.path().to_string_lossy().to_string();

    // First spawn is allowed and burns $0.50 (self-reported), putting the fleet
    // over the $0.30 cap.
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-1", "harness": "fake", "model": "haiku"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for it to complete and its cost to land in the registry.
    let mut done = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["cost_usd"].as_f64().unwrap_or(0.0) >= 0.30 {
            done = true;
            break;
        }
    }
    assert!(done, "first agent did not record its spend");

    // Second spawn must be REFUSED — the fleet cap is hit.
    let refused = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-2", "harness": "fake", "model": "haiku"}),
        )
        .await;
    assert!(
        refused.is_err(),
        "second spawn should be refused once fleet cap is hit, got {refused:?}"
    );
    let msg = format!("{}", refused.unwrap_err());
    assert!(
        msg.contains("fleet budget cap hit") || msg.contains("dispatch refused"),
        "error names the fleet cap: {msg}"
    );

    // The refusal surfaced a fleet obstacle for `rk inbox`.
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
        kinds.contains(&"budget_fleet_exceeded".to_string()),
        "fleet-exceeded obstacle posted: {kinds:?}"
    );

    // `rk cost --fleet` rollup reflects the spend and the exceeded status.
    let rollup = client.call("budget.rollup", json!({})).await.unwrap();
    assert!(rollup["fleet"]["spent_usd"].as_f64().unwrap() >= 0.30);
    assert_eq!(rollup["fleet"]["cap_usd"].as_f64().unwrap(), 0.30);
    assert_eq!(rollup["fleet"]["status"].as_str().unwrap(), "exceeded");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// TKT-39: a dismissed agent's spend must drop off the fleet tally. Spend
/// counts until the agent is dismissed, so the fleet/repo cap is a standing
/// guardrail on the current (not-yet-torn-down) fleet — not a cumulative
/// lifetime ceiling that would refuse ALL spawns once lifetime spend crossed
/// the cap. This proves both directions in one run: an undismissed spender
/// still counts (2nd spawn refused), and once it is dismissed its spend drops
/// off (3rd spawn allowed again).
#[tokio::test]
async fn dismissed_agent_drops_off_fleet_tally() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("f"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", SPENDER_FAKE);
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let daemon = Daemon::with_fleet_budget_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget {
            max_usd: 100.0,
            max_tokens: 0,
            warn_at: 0.8,
        },
        FleetBudget {
            fleet_max_usd: 0.30,
            repo_max_usd: 0.0,
            warn_at: 0.8,
        },
        space,
    )
    .unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let repo = repo_dir.path().to_string_lossy().to_string();

    // First spawn burns $0.50, putting the live fleet over the $0.30 cap.
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-1", "harness": "fake", "model": "haiku"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for its cost to land in the registry.
    let mut done = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["cost_usd"].as_f64().unwrap_or(0.0) >= 0.30 {
            done = true;
            break;
        }
    }
    assert!(done, "first agent did not record its spend");

    // While it is still undismissed its spend counts: a second spawn is refused.
    let refused = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-2", "harness": "fake", "model": "haiku"}),
        )
        .await;
    assert!(
        refused.is_err(),
        "undismissed agent's spend must count — 2nd spawn should be refused, got {refused:?}"
    );

    // Dismiss it: the record lingers (state → dismissed) but leaves the live
    // fleet, so its spend must drop off the tally.
    client
        .call("agent.dismiss", json!({"name": name, "no_merge": true}))
        .await
        .unwrap();

    // The fleet rollup now reads $0 spent and is back to ok — the dismissed
    // agent no longer counts even though its record is still registered.
    let rollup = client.call("budget.rollup", json!({})).await.unwrap();
    assert_eq!(
        rollup["fleet"]["spent_usd"].as_f64().unwrap(),
        0.0,
        "dismissed agent must drop off the fleet tally: {rollup}"
    );
    assert_eq!(rollup["fleet"]["status"].as_str().unwrap(), "ok");

    // And a fresh spawn is allowed again — the cap tracks the live fleet, not
    // cumulative lifetime spend.
    let allowed = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-3", "harness": "fake", "model": "haiku"}),
        )
        .await;
    assert!(
        allowed.is_ok(),
        "after dismissal the cap is clear — 3rd spawn should be allowed, got {allowed:?}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
