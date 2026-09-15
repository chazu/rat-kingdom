//! TKT-bahov-lakat-darif (P9.3): the one required real bounded product
//! journey — enable `landing-need-retirement`, land a real correction so it
//! emits a real telemetry event, configure+activate continuous assessment
//! over that source, advance the evaluator, inspect the published
//! assessment, then disable. No synthetic tuple stands in for the source
//! telemetry: it comes from an actual `repo.land` accepted delivery, exactly
//! as `landing_need_retirement.rs` proves for the underlying feature alone.

mod support;

use rk_core::config::Config;
use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
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

fn objective_json() -> serde_json::Value {
    json!({
        "objective_id": "rk-retirement-observed-operation-success",
        "objective_version": 1,
        "feature": "landing-need-retirement",
        "required_ratio": 1.0,
        "minimum_denominator": 1,
        "minimum_distinct_resolved_needs_for_pass": 1,
        "evaluation_cadence_seconds": 30,
        "maximum_source_to_assessment_ticks": 2,
        "source_freshness_seconds": 1800,
        "page_limit": 128,
        "maximum_pages_per_tick": 2,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_retirement_delivery_is_reflected_as_a_pass_within_two_ticks_then_disables() {
    let home = tempfile::tempdir().unwrap();
    Layout::at(home.path()).ensure().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("assessfixture");
    std::fs::create_dir(&repo).unwrap();
    init_repo_with_marker_gate(&repo);
    let repo_name = "assessfixture";

    let layout = Layout::at(home.path());
    let config = Config::default();
    let daemon = Daemon::new(layout.clone(), &config).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, &repo).await;

    // Reject malformed config outright, with no partial activation.
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

    // Valid configure, then activate.
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

    // Zero observations so far: inconclusive, not a fabricated pass/fail.
    let tick0 = client
        .call("bbs.assessment.tick", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(tick0["events_consumed"], 0, "{tick0}");
    assert_eq!(tick0["verdict"], "inconclusive", "{tick0}");

    // Enable the underlying feature and drive one real failing landing then
    // one real accepted correction, exactly like
    // `landing_need_retirement.rs`.
    client
        .call(
            "bbs.retirement.set",
            json!({"repo": repo_name, "mode": "on"}),
        )
        .await
        .unwrap();

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

    // Two evaluator ticks is the declared bound to reflect a completed
    // native source event; the accepted retirement pass's telemetry is
    // already durable by the time `repo.land` returned above, so one tick
    // suffices here — assert within the declared bound, not tighter than it.
    let mut last_tick = None;
    for _ in 0..2 {
        let tick = client
            .call("bbs.assessment.tick", json!({"repo": repo_name}))
            .await
            .unwrap();
        let verdict = tick["verdict"].as_str().unwrap().to_string();
        last_tick = Some(tick);
        if verdict == "pass" {
            break;
        }
    }
    let last_tick = last_tick.unwrap();
    assert_eq!(
        last_tick["verdict"], "pass",
        "the real accepted delivery's telemetry must be reflected within 2 ticks: {last_tick}"
    );

    let latest = client
        .call("bbs.assessment.latest", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(latest["latest"]["verdict"], "pass", "{latest}");
    assert_eq!(latest["latest"]["numerator"], 1, "{latest}");
    assert_eq!(latest["latest"]["denominator"], 1, "{latest}");
    assert_eq!(latest["latest"]["distinct_resolved_needs"], 1, "{latest}");

    let status = client
        .call("bbs.assessment.status", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(status["state"]["resolved_total"], 1, "{status}");
    assert_eq!(status["state"]["events_consumed"], 1, "{status}");

    // Disable retains the published assessment.
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

    handle.abort();
    let _ = handle.await;
}
