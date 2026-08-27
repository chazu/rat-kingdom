//! TKT-16/39/40: hierarchical fleet/repo budget caps. Once the *live* fleet's
//! cost sum reaches its cap, new spawns are refused (the wallet kill-switch) —
//! even though each individual agent stayed under its own per-agent budget. The
//! tally counts ONLY live (`Spawning`/`Running`) agents: a record that has left
//! the live fleet — completed, failed, dismissed, or orphaned — drops off, so
//! the cap is a standing guardrail on current/concurrent spend rather than a
//! cumulative lifetime ceiling (TKT-40).

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::{Budget, FleetBudget};
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

/// A single process-global fake shared by every test in this file (setting the
/// process env var to DIFFERENT scripts across parallel tests would race —
/// mirrors COMBINED_FAKE in supervisor_sweep.rs). Every test sets the SAME
/// script and NONE ever `remove_var`s it: a `remove_var` in one test would
/// unset the fake mid-flight for a parallel test's spawning agent, which would
/// then record no cost and time out (TKT-88). Leaving the identical value set
/// for the whole process is harmless. It branches on the task, which the fake
/// harness exports as `$RK_FAKE_PROMPT`:
///
/// - `*oneshot*`: self-report a $0.50 authoritative cost via a `result`
///   message and EXIT — the agent flips to `Completed`, so under the live-only
///   rule its spend must drop off the tally. Declares `rk done` first: a clean
///   turn that never does now parks the agent as `Paused` (still live) rather
///   than `Completed`, which would wrongly keep its spend in the tally.
/// - anything else: emit ONE high-token Usage event (haiku: 200k in + 40k out
///   ≈ $0.40, over the $0.30 cap) to record cost, then `sleep 120` to stay
///   `Running` (a usage event keeps state=Running; only a result would flip it
///   to Completed). One such live rat alone puts the fleet over the cap.
fn spender_fake() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"spender-1"}'
case "$RK_FAKE_PROMPT" in
  *oneshot*)
    rk_done "done"
    echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"spender-1","total_cost_usd":0.5,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
    ;;
  *)
    echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"spending"}],"usage":{"input_tokens":200000,"output_tokens":40000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
    sleep 120
    ;;
esac
"#,
    )
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("f"), "x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
}

async fn spawn_daemon(home: &Path) -> (Layout, Client) {
    let layout = Layout::at(home);
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
    tokio::spawn(daemon.run());
    let client = connect(&layout).await;
    (layout, client)
}

/// Polling budget for the spend-recorded loops below. Spawning a real daemon +
/// agent and waiting for the first usage event to be recorded is scheduler-
/// bound, so under parallel test load (many test binaries oversubscribing the
/// CPU) it can take far longer than in isolation. A generous ceiling keeps
/// these tests reliable without slowing the happy path — every loop returns the
/// instant the condition holds (TKT-88).
const SPEND_POLL_ATTEMPTS: usize = 400; // 400 × 50ms = up to 20s
const SPEND_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Wait until agent `name`'s recorded cost crosses the $0.30 cap.
async fn wait_for_spend(client: &mut Client, name: &str) {
    for _ in 0..SPEND_POLL_ATTEMPTS {
        tokio::time::sleep(SPEND_POLL_INTERVAL).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["cost_usd"].as_f64().unwrap_or(0.0) >= 0.30 {
            return;
        }
    }
    panic!("agent {name} did not record its spend");
}

#[tokio::test]
async fn fleet_cap_refuses_dispatch_once_hit() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", spender_fake());
    let (_layout, mut client) = spawn_daemon(home.path()).await;
    support::register_repo(&mut client, repo_dir.path()).await;
    let repo = repo_dir.path().to_string_lossy().to_string();

    // First spawn is allowed and burns ≈$0.40 (usage-based), STAYS Running, and
    // puts the live fleet over the $0.30 cap.
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-1", "harness": "fake", "model": "haiku"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    wait_for_spend(&mut client, &name).await;

    // The first spender is still LIVE and over the cap, so the second spawn must
    // be REFUSED.
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

    // `rk cost --fleet` rollup reflects the live spend and the exceeded status.
    let rollup = client.call("budget.rollup", json!({})).await.unwrap();
    assert!(rollup["fleet"]["spent_usd"].as_f64().unwrap() >= 0.30);
    assert_eq!(rollup["fleet"]["cap_usd"].as_f64().unwrap(), 0.30);
    assert_eq!(rollup["fleet"]["status"].as_str().unwrap(), "exceeded");
}

/// TKT-39: dismissing a LIVE over-budget agent drops its spend off the fleet
/// tally. Proves both directions in one run: an over-budget agent that is still
/// LIVE counts (2nd spawn refused), and once it is dismissed — leaving the live
/// fleet — its spend drops off (3rd spawn allowed again).
#[tokio::test]
async fn dismissed_agent_drops_off_fleet_tally() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", spender_fake());
    let (_layout, mut client) = spawn_daemon(home.path()).await;
    support::register_repo(&mut client, repo_dir.path()).await;
    let repo = repo_dir.path().to_string_lossy().to_string();

    // First spawn burns ≈$0.40 and STAYS Running, putting the live fleet over
    // the $0.30 cap.
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-1", "harness": "fake", "model": "haiku"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    wait_for_spend(&mut client, &name).await;

    // While it is still live its spend counts: a second spawn is refused.
    let refused = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-2", "harness": "fake", "model": "haiku"}),
        )
        .await;
    assert!(
        refused.is_err(),
        "live agent's spend must count — 2nd spawn should be refused, got {refused:?}"
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
}

/// TKT-40: a COMPLETED agent's spend must also drop off the fleet tally. The
/// steward lands a rat via a separate reviewer branch and never dismisses the
/// original ticket-rat, so it lingers as `Completed` — under the old rule (skip
/// only `Dismissed`) its spend kept accumulating and could silently block every
/// spawn. Now that only live agents count, a spender that records cost then
/// EXITS (Completed) drops off, so a subsequent spawn is ALLOWED even though
/// lifetime spend exceeded the cap.
#[tokio::test]
async fn completed_agent_drops_off_fleet_tally() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", spender_fake());
    let (_layout, mut client) = spawn_daemon(home.path()).await;
    support::register_repo(&mut client, repo_dir.path()).await;
    let repo = repo_dir.path().to_string_lossy().to_string();

    // The `*oneshot*` branch self-reports $0.50 and EXITS → the agent flips to
    // Completed while its cost ($0.50) exceeds the $0.30 cap.
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-oneshot", "harness": "fake", "model": "haiku"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for it to finish: cost landed AND state is `completed` (not live).
    let mut completed = false;
    for _ in 0..SPEND_POLL_ATTEMPTS {
        tokio::time::sleep(SPEND_POLL_INTERVAL).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["cost_usd"].as_f64().unwrap_or(0.0) >= 0.30
            && status["agent"]["state"].as_str() == Some("completed")
        {
            completed = true;
            break;
        }
    }
    assert!(completed, "oneshot spender did not complete with its spend");

    // Even though the completed record's lifetime spend ($0.50) exceeds the cap,
    // it has left the live fleet — the rollup reads $0 and is back to ok.
    let rollup = client.call("budget.rollup", json!({})).await.unwrap();
    assert_eq!(
        rollup["fleet"]["spent_usd"].as_f64().unwrap(),
        0.0,
        "completed agent must drop off the fleet tally: {rollup}"
    );
    assert_eq!(rollup["fleet"]["status"].as_str().unwrap(), "ok");

    // And a fresh spawn is allowed — Completed spend no longer blocks dispatch.
    let allowed = client
        .call(
            "agent.spawn",
            json!({"repo": repo, "task": "spend-again", "harness": "fake", "model": "haiku"}),
        )
        .await;
    assert!(
        allowed.is_ok(),
        "a completed agent's spend must not block a later spawn, got {allowed:?}"
    );
}
