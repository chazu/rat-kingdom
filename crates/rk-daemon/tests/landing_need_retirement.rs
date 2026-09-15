//! TKT-lalap-zonop-lafoz: a real, cheap failing native landing followed by
//! its accepted correction, proving:
//! - automatic retirement fires right after the accepted delivery lands —
//!   no `bbs.brief` call is needed for the Need to be gone (the post-landing
//!   hook in `LandingPipeline::record_delivery`, not the RPC path);
//! - a genuinely mismatched Need is never touched;
//! - the retirement (a `Resolution` provenance trail, then the Need's
//!   deletion) survives a REAL daemon restart — a fresh process reconnecting
//!   to the same persisted store, not a second call against the same
//!   in-process daemon;
//! - the default-off/on/off config lifecycle is independently observable.

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

/// Like `support::install_passing_landing_checks`, but `verify` is a real,
/// cheap, content-dependent check: `test -f READY.marker` fails against the
/// first attempt's tree (gate held, a genuine `LandingReactor::escalate`
/// fires) and passes once the correction adds that file — no synthetic
/// Tuple/`space.out` construction stands in for the failing half.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_delivery_automatically_retires_the_matching_need_and_survives_a_real_restart() {
    let home = tempfile::tempdir().unwrap();
    Layout::at(home.path()).ensure().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path().join("retirefixture");
    std::fs::create_dir(&repo).unwrap();
    init_repo_with_marker_gate(&repo);
    let repo_name = "retirefixture";

    let layout = Layout::at(home.path());
    let config = Config::default();
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, &repo).await;

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "obsolete need retirement fixture", "scope": repo_name}),
        )
        .await
        .unwrap();
    let task = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    // Attempt 1: a real, cheap failing native landing. No READY.marker, so
    // the real `verify` check fails and a genuine `LandingReactor::escalate`
    // writes the real landing Need.
    let branch = "rat/whisker-x/tkt-fix";
    git(&repo, &["checkout", "-b", branch]);
    std::fs::write(repo.join("work.txt"), "v1\n").unwrap();
    git(&repo, &["add", "work.txt"]);
    git(&repo, &["commit", "-m", "attempt 1 (gate will fail)"]);
    let f1 = git(&repo, &["rev-parse", "HEAD"]);
    git(&repo, &["checkout", "main"]);

    let first_attempt = client
        .call(
            "repo.land",
            json!({"repo": repo.to_string_lossy(), "branch": branch, "target": "main", "task": task}),
        )
        .await
        .unwrap();
    assert_ne!(
        first_attempt["merged"], true,
        "the real gate must actually fail this attempt: {first_attempt}"
    );

    let needs_after_failure = client
        .call(
            "space.scan",
            json!({"category": "need", "scope": repo_name, "identity": "landing"}),
        )
        .await
        .unwrap();
    let needs_after_failure = needs_after_failure["tuples"].as_array().unwrap();
    assert_eq!(
        needs_after_failure.len(),
        1,
        "a real gate failure must produce exactly one real landing Need: {needs_after_failure:?}"
    );
    let matching_need = needs_after_failure[0]["id"].as_str().unwrap().to_string();
    assert_eq!(
        needs_after_failure[0]["payload"]["landing_incident"]["head_sha"], f1,
        "{needs_after_failure:?}"
    );
    assert_eq!(
        needs_after_failure[0]["payload"]["landing_incident"]["branch"],
        branch
    );

    // A genuinely mismatched Need (different branch than what will actually
    // land) must never be touched. Hand-authored via `space.out` under the
    // OTHER recognized landing-need identity ("steward") so it cannot upsert
    // onto the real "landing"-identity Need above — `Need` is an evaporating
    // category, so the generic `space.out` RPC reinforces (upserts) on
    // `(category, scope, identity, instance)`; the real producer instead
    // appends via `Space::out` directly, which is why this is the ONE
    // fixture element still constructed rather than driven end to end.
    let mismatch = client
        .call(
            "space.out",
            json!({
                "category": "need", "scope": repo_name, "identity": "steward", "instance": "daemon",
                "payload": {
                    "agent": "landing", "task": task,
                    "text": "landing: run gate FAILED — branch held unmerged",
                    "landing_incident": {
                        "branch": "rat/some-other-branch/tkt-fix", "target": "main",
                        "head_sha": f1, "source_spawn": null,
                    },
                },
            }),
        )
        .await
        .unwrap();
    let mismatch_need = mismatch["id"].as_str().unwrap().to_string();

    // Enable BEFORE the correction lands: the very next successful `repo.land`
    // must retire the matching Need on its own, with no `bbs.brief` call.
    client
        .call(
            "bbs.retirement.set",
            json!({"repo": repo_name, "mode": "on"}),
        )
        .await
        .unwrap();

    // Attempt 2: the accepted correction, continuing the SAME branch
    // (f1 -> f2), so f1 is a real ancestor of the real delivered merge
    // commit.
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
    let merge_commit = landed["merge_commit"].as_str().unwrap().to_string();

    // No `bbs.brief` call yet: prove the post-landing hook alone already
    // retired the matching Need.
    let needs_now = client
        .call(
            "space.scan",
            json!({"category": "need", "scope": repo_name}),
        )
        .await
        .unwrap();
    let remaining: Vec<_> = needs_now["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !remaining.contains(&matching_need),
        "the post-landing hook alone must retire the matching Need, before any bbs.brief call: {remaining:?}"
    );
    assert!(
        remaining.contains(&mismatch_need),
        "a genuinely mismatched Need must never be touched: {remaining:?}"
    );
    assert_eq!(remaining.len(), 1, "{remaining:?}");

    let trails = client
        .call(
            "space.scan",
            json!({"category": "resolution", "scope": repo_name}),
        )
        .await
        .unwrap();
    let trails = trails["tuples"].as_array().unwrap().clone();
    assert_eq!(trails.len(), 1, "{trails:?}");
    assert_eq!(trails[0]["payload"]["need_id"], matching_need);
    assert_eq!(trails[0]["payload"]["head_sha"], f1);
    assert_eq!(trails[0]["payload"]["merge_commit"], merge_commit);
    assert_eq!(trails[0]["payload"]["branch"], branch);

    let runs = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "landing-need-retirement-run"}),
        )
        .await
        .unwrap();
    let runs = runs["tuples"].as_array().unwrap().clone();
    assert_eq!(runs.len(), 1, "one automatic post-landing pass: {runs:?}");
    assert_eq!(runs[0]["payload"]["resolved"], 1);
    assert_eq!(runs[0]["payload"]["failed"], 0);
    assert!(!runs[0]["payload"]["build"]
        .as_str()
        .unwrap_or_default()
        .is_empty());

    // A REAL daemon restart — kill this process, start a fresh one from the
    // SAME persisted layout — not merely a second call into the same
    // in-process daemon.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    let needs_after_restart = client
        .call(
            "space.scan",
            json!({"category": "need", "scope": repo_name}),
        )
        .await
        .unwrap();
    let remaining_after_restart: Vec<_> = needs_after_restart["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        remaining_after_restart,
        vec![mismatch_need.clone()],
        "the retirement must be durable across a real restart, not just this process's memory"
    );
    let trails_after_restart = client
        .call(
            "space.scan",
            json!({"category": "resolution", "scope": repo_name}),
        )
        .await
        .unwrap();
    assert_eq!(
        trails_after_restart["tuples"].as_array().unwrap().len(),
        1,
        "the provenance trail must also survive the restart"
    );

    // The `bbs.brief` RPC path is still a valid (defensive) second trigger
    // for the same idempotent pass: calling it post-restart must not error,
    // must not duplicate the trail, and must not touch the mismatch.
    client
        .call("bbs.brief", json!({"repo": repo_name, "task": task}))
        .await
        .unwrap();
    let trails_after_brief = client
        .call(
            "space.scan",
            json!({"category": "resolution", "scope": repo_name}),
        )
        .await
        .unwrap();
    assert_eq!(
        trails_after_brief["tuples"].as_array().unwrap().len(),
        1,
        "reinforce must not duplicate across triggers"
    );

    // Disable path: independently observable, and stops new housekeeping
    // while retained history (the trail/run records above) stays readable.
    let disabled = client
        .call(
            "bbs.retirement.set",
            json!({"repo": repo_name, "mode": "off"}),
        )
        .await
        .unwrap();
    assert_eq!(disabled["mode"], "off");
    let show = client
        .call("bbs.retirement.show", json!({"repo": repo_name}))
        .await
        .unwrap();
    assert_eq!(show["mode"], "off");

    handle_b.abort();
    let _ = handle_b.await;
}
