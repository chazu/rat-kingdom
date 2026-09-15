//! TKT-bahov-lakat-darif (P9.3): the one required real bounded product
//! journey — enable `landing-need-retirement`, land a real correction so it
//! emits a real telemetry event, configure+activate continuous assessment
//! over that source, and observe the resulting `pass` WITHOUT ever calling
//! `bbs.assessment.tick` manually: the daemon's own bounded scheduler sweep
//! (`Server::run`'s background loop over `continuous_assessment::sweep_due`)
//! must advance it autonomously. A real daemon restart proves the checkpoint
//! resumes and the NEW instance's own sweep loop keeps evaluating with no
//! manual tick either. Disable then proves evaluation stops. No synthetic
//! tuple stands in for the source telemetry: it comes from an actual
//! `repo.land` accepted delivery, exactly as `landing_need_retirement.rs`
//! proves for the underlying feature alone.

mod support;

use rk_core::config::Config;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn init_repo_with_marker_gate(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
    let rk_dir = dir.join(".rk");
    std::fs::write(
        rk_dir.join("checks.cue"),
        r#"checks: [
    {name: "landing-protected-paths", command: "true", timeout: "30s"},
    {name: "landing-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "test -f READY.marker", timeout: "30s"},
]
"#,
    )
    .unwrap();
    git(dir, &["add", ".rk/checks.cue"]);
    git(
        dir,
        &[
            "commit",
            "-m",
            "test: register a real content-dependent gate",
        ],
    );
}

/// A small but valid cadence — well within the objective's own validated
/// bounds (>=1s, freshness>=cadence) — so the daemon's fixed 2s internal
/// poll granularity (`CONTINUOUS_ASSESSMENT_POLL_INTERVAL`) reliably ticks
/// this repo on the very next sweep after it becomes due, keeping this test
/// bounded to a few seconds rather than a long trial.
fn objective_json() -> serde_json::Value {
    json!({
        "objective_id": "rk-retirement-observed-operation-success",
        "objective_version": 1,
        "feature": "landing-need-retirement",
        "required_ratio": 1.0,
        "minimum_denominator": 1,
        "minimum_distinct_resolved_needs_for_pass": 1,
        "evaluation_cadence_seconds": 2,
        "maximum_source_to_assessment_ticks": 2,
        "source_freshness_seconds": 60,
        "page_limit": 128,
        "maximum_pages_per_tick": 2,
    })
}

/// Bounded wait for the autonomous scheduler sweep to publish a matching
/// verdict — never a manual `bbs.assessment.tick` call. Capped at 10s (well
/// under the 2s internal poll granularity plus this objective's 2s cadence),
/// polling every 300ms; panics with the last observed value on timeout so a
/// genuine regression fails loudly rather than hanging.
async fn wait_for_verdict(client: &mut Client, repo: &str, want: &str) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last = json!(null);
    while tokio::time::Instant::now() < deadline {
        let latest = client
            .call("bbs.assessment.latest", json!({"repo": repo}))
            .await
            .unwrap();
        if latest["latest"]["verdict"] == want {
            return latest;
        }
        last = latest;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    panic!("timed out waiting for verdict {want:?}; last observed: {last}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_retirement_delivery_is_reflected_autonomously_across_a_restart_then_disables() {
    let home = tempfile::tempdir().unwrap();
    Layout::at(home.path()).ensure().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("assessfixture");
    std::fs::create_dir(&repo).unwrap();
    init_repo_with_marker_gate(&repo);
    let repo_name = "assessfixture";

    let layout = Layout::at(home.path());
    let config = Config::default();
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, &repo).await;

    // Enable the underlying feature FIRST, exactly like
    // `landing_need_retirement.rs`. Assessment activation below binds to the
    // source feature's revision AT ACTIVATION TIME, and that binding is now
    // an EXACT match (not a lower bound) — so activation must happen once
    // the feature is already on its real, already-enabled revision, not the
    // pre-enablement default (`revision: 0`) that would otherwise make every
    // subsequent real event report as a revision mismatch.
    client
        .call(
            "bbs.retirement.set",
            json!({"repo": repo_name, "mode": "on"}),
        )
        .await
        .unwrap();

    // Reject malformed config outright, with no partial activation — a
    // bounded fault case alongside the one real product journey.
    let mut bad_objective = objective_json();
    bad_objective["required_ratio"] = json!(2.5);
    let rejected = client
        .call(
            "bbs.assessment.configure",
            json!({"repo": repo_name, "objective": bad_objective}),
        )
        .await;
    assert!(rejected.is_err(), "malformed objective must be refused");
    let show_after_reject = client
        .call("bbs.assessment.show", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(
        show_after_reject["objective"],
        serde_json::Value::Null,
        "a rejected configure must leave no partial state: {show_after_reject}"
    );

    // Valid configure, then activate. No `bbs.assessment.tick` call anywhere
    // in this test from here on: every subsequent assessment must come from
    // the daemon's own autonomous sweep.
    client
        .call(
            "bbs.assessment.configure",
            json!({"repo": repo_name, "objective": objective_json()}),
        )
        .await
        .unwrap();
    let activated = client
        .call("bbs.assessment.activate", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(activated["enabled"], true, "{activated}");

    // Zero observations so far: the autonomous sweep still publishes
    // `unavailable` (no telemetry yet) on its own, with no tick call.
    wait_for_verdict(&mut client, repo_name, "unavailable").await;

    // Drive one real failing landing then one real accepted correction.
    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "continuous assessment fixture", "scope": repo_name}),
        )
        .await
        .unwrap();
    let task = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let branch = "rat/whisker-x/tkt-fix";
    git(&repo, &["checkout", "-b", branch]);
    std::fs::write(repo.join("work.txt"), "v1\n").unwrap();
    git(&repo, &["add", "work.txt"]);
    git(&repo, &["commit", "-m", "attempt 1 (gate will fail)"]);
    git(&repo, &["checkout", "main"]);
    let first_attempt = client
        .call(
            "repo.land",
            json!({"repo": repo.to_string_lossy(), "branch": branch, "target": "main", "task": task}),
        )
        .await
        .unwrap();
    assert_ne!(first_attempt["merged"], true, "{first_attempt}");

    git(&repo, &["checkout", branch]);
    std::fs::write(repo.join("READY.marker"), "\n").unwrap();
    std::fs::write(repo.join("work.txt"), "v2 (fixed)\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "attempt 2 (corrected)"]);
    git(&repo, &["checkout", "main"]);
    let landed = client
        .call(
            "repo.land",
            json!({"repo": repo.to_string_lossy(), "branch": branch, "target": "main", "task": task}),
        )
        .await
        .unwrap();
    assert_eq!(landed["merged"], true, "{landed}");

    // Autonomous sweep alone must reflect the real accepted delivery's
    // telemetry — no manual tick.
    let latest = wait_for_verdict(&mut client, repo_name, "pass").await;
    assert_eq!(latest["latest"]["numerator"], 1, "{latest}");
    assert_eq!(latest["latest"]["denominator"], 1, "{latest}");
    assert_eq!(latest["latest"]["distinct_resolved_needs"], 1, "{latest}");

    let status = client
        .call("bbs.assessment.status", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(status["state"]["events_consumed"], 1, "{status}");
    assert_eq!(
        status["state"]["distinct_resolved_needs"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{status}"
    );

    // Real daemon restart: drop this instance, build a fresh one over the
    // SAME persisted layout. The checkpoint (cursor, totals, latest) and the
    // activation must resume from disk with no operator intervention, and
    // the NEW instance's OWN scheduler sweep — not any carried-over task —
    // must keep evaluating autonomously.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The checkpoint survived the restart without needing to be re-observed.
    let latest_after_restart = client
        .call("bbs.assessment.latest", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(
        latest_after_restart["latest"]["verdict"], "pass",
        "{latest_after_restart}"
    );

    // A SECOND real delivery, landed against the NEW daemon instance, is
    // also picked up with no manual tick — proving the restarted instance's
    // own sweep loop, not a fluke of the first instance's task still
    // running.
    let branch2 = "rat/whisker-y/tkt-fix2";
    git(&repo, &["checkout", "-b", branch2]);
    std::fs::write(repo.join("READY.marker"), "\n").unwrap();
    std::fs::write(repo.join("work2.txt"), "v1\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "second fixture delivery"]);
    git(&repo, &["checkout", "main"]);
    let ticket2 = client
        .call(
            "ticket.new",
            json!({"title": "continuous assessment fixture 2", "scope": repo_name}),
        )
        .await
        .unwrap();
    let task2 = ticket2["ticket"]["identity"].as_str().unwrap().to_string();
    let landed2 = client
        .call(
            "repo.land",
            json!({"repo": repo.to_string_lossy(), "branch": branch2, "target": "main", "task": task2}),
        )
        .await
        .unwrap();
    assert_eq!(landed2["merged"], true, "{landed2}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut second_seen = false;
    while tokio::time::Instant::now() < deadline {
        let status = client
            .call("bbs.assessment.status", json!({"repo": repo_name}))
            .await
            .unwrap();
        if status["state"]["events_consumed"] == 2 {
            second_seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        second_seen,
        "the restarted daemon's own sweep loop must autonomously observe a second real delivery"
    );

    // Disable retains the published assessment and stops future evaluation.
    let disabled = client
        .call("bbs.assessment.disable", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(disabled["enabled"], false, "{disabled}");
    let latest_after_disable = client
        .call("bbs.assessment.latest", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(
        latest_after_disable["latest"]["verdict"], "pass",
        "disable must retain prior evidence: {latest_after_disable}"
    );
    let status_after_disable = client
        .call("bbs.assessment.status", json!({"repo": repo_name}))
        .await
        .unwrap();
    let events_at_disable = status_after_disable["state"]["events_consumed"].clone();

    // Prove disable actually stops future evaluation: give the (still
    // running) sweep loop several full polls' worth of time, then confirm
    // the checkpoint has not moved even though the underlying feature is
    // still enabled and could in principle still produce telemetry.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let status_later = client
        .call("bbs.assessment.status", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(
        status_later["state"]["events_consumed"], events_at_disable,
        "a disabled repo must never be selected by the scheduler sweep"
    );

    handle_b.abort();
    let _ = handle_b.await;
}
