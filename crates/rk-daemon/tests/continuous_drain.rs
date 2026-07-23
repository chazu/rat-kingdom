//! Continuous-drain end to end: a WIP-limited fleet autoscaler that REFILLS.
//!
//! Where `backlog-drain` fans out once, this keeps `max_wip` rats live and
//! spawns the next ready ticket the moment a slot frees. The test drives a
//! backlog deeper than the cap through a slow fake harness and asserts:
//!   - the live count never exceeds `max_wip` (the cap holds);
//!   - every ready ticket is eventually dispatched and reaches `done` (refill);
//!   - each ticket is dispatched exactly once (atomic claim, no double-grab);
//!   - a system-scope ticket (no registered repo) is never dispatched.

use rk_core::config::DrainConfig;
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
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
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

// A rat that works for ~0.4s before reporting a clean success — long enough that
// its live window is reliably observable across 50ms polls, so a WIP cap that is
// respected keeps the observed live count at or below the target.
const SLOW_FAKE: &str = r#"
read -r _prompt
sleep 0.4
echo '{"type":"system","subtype":"init","session_id":"drain-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"drained","session_id":"drain-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test]
async fn continuous_drain_refills_up_to_wip_and_never_exceeds_it() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    std::env::set_var("RK_FAKE_HARNESS_CMD", SLOW_FAKE);
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    // Unlimited budget so ONLY the WIP cap governs concurrency here.
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_drain_config(DrainConfig {
        enabled: true,
        max_wip: 2,
        interval_secs: 1,
        repo: None,
        aging_secs: 3600,
    });
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Register the repo so the drain can resolve a ticket's scope → worktree.
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // Five ready tickets — deeper than the WIP cap of 2, so the loop must refill.
    for i in 0..5 {
        client
            .call(
                "ticket.new",
                json!({"title": format!("task {i}"), "body": "do it", "scope": repo_name}),
            )
            .await
            .unwrap();
    }
    // A system-scope ticket (default scope) resolves to no registered repo and
    // must never be dispatched.
    client
        .call("ticket.new", json!({"title": "orphan"}))
        .await
        .unwrap();

    // Poll the fleet: track the peak live count and wait for all five to finish.
    let mut peak_live = 0usize;
    let mut all_done = false;
    for _ in 0..200 {
        let agents = client.call("agent.list", json!({})).await.unwrap();
        let live = agents["agents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| matches!(a["state"].as_str(), Some("spawning") | Some("running")))
            .count();
        peak_live = peak_live.max(live);

        let tickets = client
            .call("ticket.list", json!({"scope": repo_name}))
            .await
            .unwrap();
        let done = tickets["tickets"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["payload"]["status"] == "done")
            .count();
        if done == 5 {
            all_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(all_done, "all five ready tickets should be drained to done");
    assert!(
        peak_live >= 1 && peak_live <= 2,
        "WIP cap of 2 must hold: peak live was {peak_live}"
    );

    // Exactly five rats spawned — one per ticket, no ticket double-grabbed.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let list = agents["agents"].as_array().unwrap();
    assert_eq!(
        list.len(),
        5,
        "one rat per ready ticket, dispatched once each"
    );

    // The system-scope ticket was left untouched (no repo to dispatch into).
    let orphan = client
        .call("ticket.list", json!({"scope": "system"}))
        .await
        .unwrap();
    assert_eq!(orphan["tickets"][0]["payload"]["status"], "open");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
