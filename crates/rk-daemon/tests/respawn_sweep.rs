//! TKT-53: self-healing respawn with crash-loop backoff.
//!
//! The liveness sweep (TKT-15) also auto-`respawn`s agents that crashed out of
//! their run, bounded by an attempt cap + exponential backoff so a
//! genuinely-broken task cannot respawn-loop forever. Once the cap is hit it
//! escalates a `need` (surfaced by `rk inbox`) for a human and stops.
//!
//! This drives the whole loop end to end over the socket: a fake that always
//! crashes is spawned, the sweep respawns it up to the cap, then escalates —
//! and the respawn count never exceeds the cap.

use rk_core::config::SupervisorConfig;
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

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("f"), "x\n").unwrap();
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

/// Count `agent_respawned` events emitted for `name`.
async fn respawn_count(client: &mut Client, name: &str) -> usize {
    let events = client
        .call("space.scan", json!({"category": "event"}))
        .await
        .unwrap();
    events["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| {
            t["identity"] == "agent_respawned" && t["payload"]["agent"] == name
        })
        .count()
}

#[tokio::test]
async fn crashed_agent_auto_respawns_up_to_cap_then_escalates() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    // A fake that dies immediately (no result event, no commit) — a crash that
    // leaves the agent Failed every launch. It never commits, so its branch
    // equals its base and the merged-branch guardrail does NOT skip it.
    std::env::set_var("RK_FAKE_HARNESS_CMD", "read -r _p; exit 3");

    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    // Self-heal on, cap 2, no backoff so retries fire each sweep. Stuck/burn
    // detection off — this test is only about the respawn arm.
    daemon.set_sweep_config(SupervisorConfig {
        enabled: true,
        interval_secs: 1,
        stuck_after_secs: 0,
        burn_usd_per_min: 0.0,
        kill_grace_secs: 1,
        respawn_enabled: true,
        respawn_max_attempts: 2,
        respawn_backoff_secs: 0,
        ..SupervisorConfig::default()
    });
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "doomed",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // The sweep exhausts the cap and escalates a `respawn_exhausted` need.
    let mut escalated = false;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let needs = client
            .call("space.scan", json!({"category": "need"}))
            .await
            .unwrap();
        escalated = needs["tuples"].as_array().unwrap().iter().any(|t| {
            t["payload"]["type"] == "respawn_exhausted" && t["payload"]["agent"] == name
        });
        if escalated {
            break;
        }
    }
    assert!(escalated, "cap-exhausted agent never escalated a need");

    // The bound held: exactly `respawn_max_attempts` respawns, no more — even
    // after settling for a couple more sweeps.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        respawn_count(&mut client, &name).await,
        2,
        "respawns must not exceed the attempt cap"
    );

    // Only one need is escalated for the exhausted agent (no re-escalation).
    let needs = client
        .call("space.scan", json!({"category": "need"}))
        .await
        .unwrap();
    let exhausted_needs = needs["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| {
            t["payload"]["type"] == "respawn_exhausted" && t["payload"]["agent"] == name
        })
        .count();
    assert_eq!(exhausted_needs, 1, "cap should escalate exactly one need");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
