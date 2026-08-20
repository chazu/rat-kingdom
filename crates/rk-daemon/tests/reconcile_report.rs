//! End-to-end coverage for `reconcile.report` (`crate::reconcile`): the RPC
//! assembly and the real git subprocess calls it depends on
//! (`Daemon::cleared_branches`, `Repo::is_ancestor`) must actually work
//! together, not just the pure `build()` unit tests in
//! `rk-daemon/src/reconcile.rs`.
//!
//! Reproduces TKT-171's "currently observed" contradiction family directly:
//! a `land` step reports a clean `{merged: false, pr_opened: false}` when a
//! merge conflicts, leaving finished work stranded outside its target with
//! nothing else in the fleet tracking it.

mod support;

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
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_land_surfaces_as_a_conflict_held_violation_and_self_clears() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path();
    git(repo_path, &["init", "-b", "main"]);
    git(repo_path, &["config", "user.email", "r@x"]);
    git(repo_path, &["config", "user.name", "R"]);
    std::fs::write(repo_path.join("README.md"), "# x\n").unwrap();
    git(repo_path, &["add", "."]);
    git(repo_path, &["commit", "-m", "init"]);
    git(repo_path, &["checkout", "-b", "rat/whisker/work"]);
    std::fs::write(repo_path.join("README.md"), "# rat version\n").unwrap();
    git(repo_path, &["commit", "-am", "rat work"]);
    git(repo_path, &["checkout", "main"]);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo_name = "reconcile-repo".to_string();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_path.to_string_lossy()}),
        )
        .await
        .unwrap();

    // Reading the report before anything has happened must be safe and
    // report a clean repo — no violations invented out of an empty state.
    let clean = client
        .call("reconcile.report", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(
        clean["violations"].as_array().unwrap().len(),
        0,
        "an untouched repo must converge cleanly: {clean}"
    );

    // The exact event shape `land` writes for a conflict it deliberately
    // reports as clean rather than an error (TKT-171): `content_free: false`
    // (the branch really did change something) is what makes this event
    // "content proven" and worth asking git about at all.
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo_name,
                "identity": "branch_landed",
                "payload": {
                    "branch": "rat/whisker/work",
                    "target": "main",
                    "merged": false,
                    "pr_opened": false,
                    "content_free": false,
                    "detail": "conflict in README.md",
                },
                "lifecycle": "furniture",
            }),
        )
        .await
        .unwrap();

    let report = client
        .call("reconcile.report", json!({"repo": repo_name}))
        .await
        .unwrap();
    let violations = report["violations"].as_array().cloned().unwrap_or_default();
    let hit = violations
        .iter()
        .find(|v| v["kind"] == "conflict-held-landing" && v["subject"] == "rat/whisker/work")
        .unwrap_or_else(|| panic!("expected a conflict-held-landing row: {report}"));
    assert_eq!(hit["authority"], "orchestrator");
    assert!(hit["id"].as_str().unwrap().contains("rat/whisker/work"));
    assert!(!hit["evidence"].as_array().unwrap().is_empty());

    // Reading twice over unchanged state must be byte-identical (no side
    // effects, no hidden clock/ordering dependence).
    let report_again = client
        .call("reconcile.report", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(report, report_again);

    // Now hand-merge the branch exactly as an operator resolving the
    // conflict would, and the row must self-clear — nothing has to write a
    // "resolved" marker, matching `rk inbox`'s own unlanded-branch check.
    git(
        repo_path,
        &["merge", "--no-ff", "-m", "resolve", "rat/whisker/work"],
    );

    let cleared = client
        .call("reconcile.report", json!({"repo": repo_name}))
        .await
        .unwrap();
    let still_present = cleared["violations"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["kind"] == "conflict-held-landing" && v["subject"] == "rat/whisker/work");
    assert!(
        !still_present,
        "a hand-merged branch must self-clear: {cleared}"
    );
}
