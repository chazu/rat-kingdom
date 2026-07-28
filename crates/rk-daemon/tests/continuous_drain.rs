//! Continuous-drain end to end: a WIP-limited fleet autoscaler that REFILLS.
//!
//! Where `backlog-drain` fans out once, this keeps `max_wip` rats live and
//! spawns the next ready ticket the moment a slot frees. The test drives a
//! backlog deeper than the cap through a slow fake harness and asserts:
//!   - the live count never exceeds `max_wip` (the cap holds);
//!   - every ready ticket is eventually dispatched and reaches `done` (refill);
//!   - each ticket is dispatched exactly once (atomic claim, no double-grab);
//!   - a system-scope ticket (no registered repo) is never dispatched.
//!
//! Both tests here drive a live daemon that spawns real worktrees, so they are
//! scheduler-bound: waits are bounded by a wall-clock [`DRAIN_DEADLINE`] rather
//! than an iteration count, concurrency caps are asserted as upper bounds on a
//! sampled peak, and anything that must hold exactly is read once the backlog
//! has settled. See [`SLOW_FAKE`] for why neither test unsets the fake harness.

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
//
// BOTH tests in this file set this SAME script into `RK_FAKE_HARNESS_CMD` and
// NEITHER ever `remove_var`s it. The variable is process-global and cargo runs
// these two tests concurrently in one process, so a `remove_var` on the exit
// path of whichever test finishes first unsets the fake for the other test's
// still-pending spawns: those rats run no script, never declare `rk_done`, and
// their tickets never reach `done` — the peer then burns its whole wait budget
// and fails. That is TKT-183, and it is the same race TKT-88 fixed in
// fleet_budget.rs (supervisor_sweep.rs never removes it for the same reason).
// Leaving one identical value set for the life of the process is harmless.
const SLOW_FAKE: &str = r#"
read -r _prompt
sleep 0.4
echo '{"type":"system","subtype":"init","session_id":"drain-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"drained","session_id":"drain-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Wall-clock ceiling for the refill loops below, and the gap between polls.
///
/// A loop *iteration count* is not a time budget: each pass also issues two or
/// three RPCs against a daemon that is concurrently doing real `git worktree
/// add`s and process launches, so the same 300 passes cover far less real time
/// under parallel test load than in isolation — precisely when they need to
/// cover more. A deadline means what it says and does not shrink under
/// contention. Sized with the headroom TKT-88 established for these
/// scheduler-bound daemon+agent polls, scaled to the several rats each test
/// here waits on *sequentially*. Every loop breaks the instant its condition
/// holds, so the happy path (~2s) is unchanged.
const DRAIN_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

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
    let deadline = tokio::time::Instant::now() + DRAIN_DEADLINE;
    while tokio::time::Instant::now() < deadline {
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
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert!(all_done, "all five ready tickets should be drained to done");
    // The cap is the invariant, and it is an UPPER bound. A lower bound here
    // would only assert that a 50ms sampler happened to catch one of the ~0.4s
    // live windows, which is sampling luck, not behaviour — that rats really
    // ran is settled below by `all_done` plus the exact agent count (TKT-183).
    assert!(
        peak_live <= 2,
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
    // NOTE: deliberately no `remove_var("RK_FAKE_HARNESS_CMD")` — see SLOW_FAKE.
}

/// Cross-repo WIP partitioning: the `repos` map is an allowlist whose per-repo
/// caps subdivide the fleet-wide ceiling, so one busy repo cannot monopolize the
/// fleet. Two allowlisted repos are each capped at one live rat; a third
/// registered-but-unlisted repo is never drained. Asserts:
///   - neither allowlisted repo ever holds more than its cap of 1 rat live;
///   - both allowlisted repos' backlogs drain to `done` (fair progress);
///   - each repo ends with exactly one rat per ticket (no double-grab);
///   - the unlisted repo's tickets stay open and it gets no rat at all.
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
    let deadline = tokio::time::Instant::now() + DRAIN_DEADLINE;
    while tokio::time::Instant::now() < deadline {
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
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert!(both_done, "both allowlisted repos should drain to done");
    // Each per-repo cap is an UPPER bound on concurrency, so that is what the
    // sampled peak asserts. Requiring `== 1` also demanded that the 50ms poll
    // caught one of the ~0.4s live windows; under parallel load this loop's own
    // three RPCs per pass can stretch past a whole window, so that half of the
    // assert tested the sampler, not the cap (TKT-183).
    assert!(
        peak_alpha <= 1,
        "alpha's per-repo cap of 1 must hold, but peak live was {peak_alpha}"
    );
    assert!(
        peak_beta <= 1,
        "beta's per-repo cap of 1 must hold, but peak live was {peak_beta}"
    );

    // The settled per-repo agent tally — read after both backlogs drained, so
    // it is a fact rather than a sample. This is what the sampled peak cannot
    // prove: one rat per ticket in each allowlisted repo (no ticket
    // double-grabbed), that `repo_name` really is the key the per-repo caps
    // partition on, and that the excluded repo was never dispatched into at all
    // — a stronger statement than gamma's tickets merely staying open.
    assert_eq!(
        agents_for_repo(&mut client, &alpha).await,
        3,
        "one rat per alpha ticket, dispatched once each"
    );
    assert_eq!(
        agents_for_repo(&mut client, &beta).await,
        3,
        "one rat per beta ticket, dispatched once each"
    );
    assert_eq!(
        agents_for_repo(&mut client, &gamma).await,
        0,
        "unlisted repo must never have a rat dispatched into it"
    );

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
    // NOTE: deliberately no `remove_var("RK_FAKE_HARNESS_CMD")` — see SLOW_FAKE.
}

/// How many agent records the fleet holds for `scope`. Terminal records stay in
/// the default `agent.list` view until an explicit `agent.archive` (TKT-136),
/// and nothing archives on a timer, so after the drain settles this is a stable
/// count rather than a race against completion.
async fn agents_for_repo(client: &mut Client, scope: &str) -> usize {
    let agents = client.call("agent.list", json!({})).await.unwrap();
    agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["repo_name"].as_str() == Some(scope))
        .count()
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
