//! Continuous-drain end to end: a WIP-limited fleet autoscaler that REFILLS.
//!
//! Where `backlog-drain` fans out once, this keeps `max_wip` rats live and
//! spawns the next ready ticket the moment a slot frees. The test drives a
//! backlog deeper than the cap through a slow fake harness and asserts:
//!   - the live count never exceeds `max_wip` (the cap holds);
//!   - every ready ticket is eventually dispatched and reaches `done` (refill);
//!   - each ticket is dispatched exactly once (atomic claim, no double-grab);
//!   - a system-scope ticket (no registered repo) is never dispatched.

mod fixture;

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
rk_done "work done"   # a rat that never declares done fails (TKT-175)
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

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(SLOW_FAKE));
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
        repos: std::collections::HashMap::new(),
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
        (1..=2).contains(&peak_live),
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

/// Cross-repo WIP partitioning: the `repos` map is an allowlist whose per-repo
/// caps subdivide the fleet-wide ceiling, so one busy repo cannot monopolize the
/// fleet. Two allowlisted repos are each capped at one live rat; a third
/// registered-but-unlisted repo is never drained. Asserts:
///   - neither allowlisted repo ever holds more than its cap of 1 rat live;
///   - both allowlisted repos' backlogs drain to `done` (fair progress);
///   - the unlisted repo's tickets stay open (allowlist excludes it).
#[tokio::test]
async fn partition_caps_hold_per_repo_and_allowlist_excludes_unlisted() {
    let home = tempfile::tempdir().unwrap();
    let make_repo = || {
        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        let name = dir
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        (dir, name)
    };
    let (alpha_dir, alpha) = make_repo();
    let (beta_dir, beta) = make_repo();
    let (gamma_dir, gamma) = make_repo();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(SLOW_FAKE));
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
    // Fleet ceiling of 4 would let either repo run away on its own; the per-repo
    // cap of 1 is what must bind. Gamma is deliberately absent from the map, so
    // the allowlist excludes it entirely.
    let mut repos = std::collections::HashMap::new();
    repos.insert(
        alpha.clone(),
        rk_core::config::RepoDrainConfig {
            enabled: true,
            max_wip: 1,
        },
    );
    repos.insert(
        beta.clone(),
        rk_core::config::RepoDrainConfig {
            enabled: true,
            max_wip: 1,
        },
    );
    daemon.set_drain_config(DrainConfig {
        enabled: true,
        max_wip: 4,
        interval_secs: 1,
        repo: None,
        repos,
        aging_secs: 3600,
    });
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    for (name, dir) in [
        (&alpha, &alpha_dir),
        (&beta, &beta_dir),
        (&gamma, &gamma_dir),
    ] {
        client
            .call(
                "repo.add",
                json!({"name": name, "path": dir.path().to_string_lossy()}),
            )
            .await
            .unwrap();
        for i in 0..3 {
            client
                .call(
                    "ticket.new",
                    json!({"title": format!("{name} task {i}"), "body": "do it", "scope": name}),
                )
                .await
                .unwrap();
        }
    }

    // Poll: track the peak live count PER repo and wait for the two allowlisted
    // repos to finish (3 done each).
    let mut peak_alpha = 0usize;
    let mut peak_beta = 0usize;
    let mut both_done = false;
    for _ in 0..300 {
        let agents = client.call("agent.list", json!({})).await.unwrap();
        let mut live_alpha = 0usize;
        let mut live_beta = 0usize;
        for a in agents["agents"].as_array().unwrap() {
            if !matches!(a["state"].as_str(), Some("spawning") | Some("running")) {
                continue;
            }
            match a["repo_name"].as_str() {
                Some(r) if r == alpha => live_alpha += 1,
                Some(r) if r == beta => live_beta += 1,
                _ => {}
            }
        }
        peak_alpha = peak_alpha.max(live_alpha);
        peak_beta = peak_beta.max(live_beta);

        let a_done = ticket_done_count(&mut client, &alpha).await;
        let b_done = ticket_done_count(&mut client, &beta).await;
        if a_done == 3 && b_done == 3 {
            both_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(both_done, "both allowlisted repos should drain to done");
    assert_eq!(peak_alpha, 1, "alpha cap of 1 must hold (peak {peak_alpha})");
    assert_eq!(peak_beta, 1, "beta cap of 1 must hold (peak {peak_beta})");

    // Gamma is registered but not in the allowlist → its backlog is untouched.
    let gamma_tickets = client
        .call("ticket.list", json!({"scope": gamma}))
        .await
        .unwrap();
    let gamma_open = gamma_tickets["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["payload"]["status"] == "open")
        .count();
    assert_eq!(gamma_open, 3, "unlisted repo must never be drained");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

async fn ticket_done_count(client: &mut Client, scope: &str) -> usize {
    let tickets = client
        .call("ticket.list", json!({"scope": scope}))
        .await
        .unwrap();
    tickets["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["payload"]["status"] == "done")
        .count()
}
