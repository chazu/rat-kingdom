//! TKT-65: PR-mode dismiss. A repo registered with `merge_mode = pr` must,
//! on dismiss, push the rat's branch and open a pull request against the base
//! rather than merging it — leaving the branch standing for review, reporting
//! `{merged: false, pr_opened: true}`, and never touching the base branch.
//!
//! This is the counterpart to `merge_queue.rs`, which proves the Direct path:
//! there dismiss merges into `main` and deletes the branch. Here the identical
//! dismiss call, differing only in the repo's registered merge mode, must take
//! the PR fork instead.

use rk_core::config::ReviewSweepConfig;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

// The rat writes a per-agent file, commits it on its branch, and reports clean.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work by $RK_AGENT for $RK_TASK" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"pr-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"pr-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pr_mode_dismiss_opens_pr_and_keeps_branch() {
    let home = tempfile::tempdir().unwrap();

    // A bare repo stands in for the remote the branch is pushed to.
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare", "-b", "main"]);

    // The working repo, forked from main, with `origin` pointing at the bare repo.
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path();
    git(repo_path, &["init", "-b", "main"]);
    git(repo_path, &["config", "user.email", "r@x"]);
    git(repo_path, &["config", "user.name", "R"]);
    git(
        repo_path,
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    std::fs::write(repo_path.join("README.md"), "# x\n").unwrap();
    git(repo_path, &["add", "."]);
    git(repo_path, &["commit", "-m", "init"]);
    git(repo_path, &["push", "-u", "origin", "main"]);
    let base_head = git(repo_path, &["rev-parse", "main"]).trim().to_string();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Register the repo in PR mode. `repo.name()` (the dir basename) is what a
    // spawned rat records as its repo_name and what dismiss resolves against.
    let repo_name = repo_path.file_name().unwrap().to_string_lossy().to_string();
    client
        .call(
            "repo.add",
            json!({
                "name": repo_name,
                "path": repo_path.to_string_lossy(),
                "merge_mode": "pr",
                "remote": "origin",
            }),
        )
        .await
        .unwrap();

    // Spawn one rat and wait for its commit.
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_path.to_string_lossy(),
                "task": "pr-1",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();

    let mut done = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            done = true;
            break;
        }
    }
    assert!(done, "rat {name} never completed");

    // Dismiss: PR mode pushes + opens a PR, never merges or deletes the branch.
    let res = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(res["merged"], false, "PR mode must not merge: {res}");
    assert_eq!(res["pr_opened"], true, "PR mode must open a PR: {res}");

    // The base branch is untouched: no work file, HEAD unmoved.
    git(repo_path, &["checkout", "main"]);
    let tracked = git(repo_path, &["ls-files"]);
    assert!(
        !tracked.lines().any(|f| f.starts_with("work-")),
        "PR mode must not merge work onto main, got: {tracked}"
    );
    assert_eq!(
        git(repo_path, &["rev-parse", "main"]).trim(),
        base_head,
        "main must not advance in PR mode"
    );

    // The branch still exists locally (not deleted) ...
    let branches = git(repo_path, &["branch", "--format=%(refname:short)"]);
    assert!(
        branches.lines().map(str::trim).any(|b| b == branch),
        "PR-mode dismiss must keep the branch {branch}, have: {branches}"
    );
    // ... and was pushed to the remote.
    let remote_ref = Command::new("git")
        .arg("-C")
        .arg(origin.path())
        .args(["rev-parse", "--verify", &format!("refs/heads/{branch}")])
        .output()
        .unwrap();
    assert!(
        remote_ref.status.success(),
        "branch {branch} must be pushed to origin: {}",
        String::from_utf8_lossy(&remote_ref.stderr)
    );

    // TKT-67: the pushed PR is visible attention — `rk inbox` surfaces it as an
    // awaiting-review row keyed on the branch, carrying the forge URL, so the
    // completed-run branch is never silently forgotten.
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    let items = inbox["items"].as_array().expect("inbox has items array");
    let review = items
        .iter()
        .find(|it| it["kind"] == "awaiting-review")
        .unwrap_or_else(|| panic!("open PR must appear as awaiting-review: {inbox}"));
    assert_eq!(
        review["subject"], branch,
        "awaiting-review row is keyed on the branch: {review}"
    );
    assert!(
        review["detail"].as_str().unwrap_or("").contains(&branch),
        "awaiting-review detail names the branch: {review}"
    );
    assert!(
        review["action"]
            .as_str()
            .unwrap_or("")
            .contains("review & merge"),
        "awaiting-review action points at the forge merge: {review}"
    );

    // TKT-69: the row auto-clears once the PR is merged. The daemon never sees
    // the forge merge directly, so it detects it locally — once the merge
    // reaches the local target branch (the operator's pull, here simulated by
    // merging the branch into main), the awaiting-review row disappears without
    // waiting for the `pull_request_opened` event to be pruned.
    git(repo_path, &["checkout", "main"]);
    git(repo_path, &["merge", "--ff-only", &branch]);
    assert_ne!(
        git(repo_path, &["rev-parse", "main"]).trim(),
        base_head,
        "sanity: main should now contain the merged branch"
    );
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    let items = inbox["items"].as_array().expect("inbox has items array");
    assert!(
        !items
            .iter()
            .any(|it| it["kind"] == "awaiting-review" && it["subject"] == branch),
        "merged PR must auto-clear from the awaiting-review queue: {inbox}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// TKT-70: a human merges the PR on the forge but the operator never pulls, so
// the LOCAL target never advances and TKT-69's local check cannot clear the
// row. The opt-in fetch-driven review sweep closes the gap: it `git fetch
// --prune`es the repo, sees the branch merged into `origin/main` (or pruned),
// emits a `pull_request_closed` event, and `rk inbox` drops the row — all
// without a local pull.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_sweep_clears_awaiting_review_on_forge_merge_without_a_pull() {
    let home = tempfile::tempdir().unwrap();

    // Bare "forge" remote + working repo pushed to it.
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare", "-b", "main"]);
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path();
    git(repo_path, &["init", "-b", "main"]);
    git(repo_path, &["config", "user.email", "r@x"]);
    git(repo_path, &["config", "user.name", "R"]);
    git(
        repo_path,
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    std::fs::write(repo_path.join("README.md"), "# x\n").unwrap();
    git(repo_path, &["add", "."]);
    git(repo_path, &["commit", "-m", "init"]);
    git(repo_path, &["push", "-u", "origin", "main"]);
    let base_head = git(repo_path, &["rev-parse", "main"]).trim().to_string();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
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
    // Fetch-driven clear on, fast cadence so the test does not wait minutes.
    daemon.set_review_sweep_config(ReviewSweepConfig {
        enabled: true,
        interval_secs: 1,
        remote: "origin".into(),
        fetch_timeout_secs: 30,
    });
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo_name = repo_path.file_name().unwrap().to_string_lossy().to_string();
    client
        .call(
            "repo.add",
            json!({
                "name": repo_name,
                "path": repo_path.to_string_lossy(),
                "merge_mode": "pr",
                "remote": "origin",
            }),
        )
        .await
        .unwrap();

    // Spawn a rat, wait for its commit, dismiss to open the PR (branch pushed).
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo_path.to_string_lossy(), "task": "pr-1", "harness": "fake"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    let mut done = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            done = true;
            break;
        }
    }
    assert!(done, "rat {name} never completed");
    let res = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(res["pr_opened"], true, "PR mode must open a PR: {res}");

    // The awaiting-review row is present.
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    assert!(
        inbox["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|it| it["kind"] == "awaiting-review" && it["subject"] == branch),
        "open PR must show as awaiting-review: {inbox}"
    );

    // A human merges the PR on the forge: advance ONLY the bare's main via a
    // throwaway clone. The working repo never pulls, so its local main stays.
    let forge = tempfile::tempdir().unwrap();
    git(
        forge.path(),
        &["clone", &origin.path().to_string_lossy(), "."],
    );
    git(forge.path(), &["config", "user.email", "f@x"]);
    git(forge.path(), &["config", "user.name", "F"]);
    git(
        forge.path(),
        &["merge", "--no-ff", "-m", "merge PR", &format!("origin/{branch}")],
    );
    git(forge.path(), &["push", "origin", "main"]);

    // The local target is unchanged — TKT-69's local check cannot clear the row.
    assert_eq!(
        git(repo_path, &["rev-parse", "main"]).trim(),
        base_head,
        "sanity: local main must NOT have advanced (no pull happened)"
    );

    // The review sweep (interval 1s) fetches, sees origin/main now contains the
    // branch, emits pull_request_closed, and the row clears.
    let mut cleared = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let inbox = client.call("inbox.list", json!({})).await.unwrap();
        let still_open = inbox["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|it| it["kind"] == "awaiting-review" && it["subject"] == branch);
        if !still_open {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "the fetch-driven sweep must clear the awaiting-review row after a forge merge with no local pull"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
