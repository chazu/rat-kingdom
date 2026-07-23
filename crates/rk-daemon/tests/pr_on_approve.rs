//! End-to-end proof of the `open_pr` step (TKT-66), exercising the real
//! `examples/workflows/pr-on-approve.cue` through the daemon + supervisor
//! against the fake harness.
//!
//! This is the PR-mode sibling of `land_on_approve.rs`. Where `land` merges the
//! reviewed branch straight onto main, `open_pr` pushes the branch and opens a
//! pull request instead — and it does so ALWAYS, regardless of the repo's merge
//! mode. To prove the "regardless of repo policy" contract, the repo here is
//! left UNREGISTERED (so the daemon resolves it to the default Direct merge
//! mode): a plain `land`/`dismiss` would merge onto main, but `open_pr` must
//! still hand the branch off as a PR — main untouched, branch pushed + kept.
//! On REJECT nothing is pushed and the run still COMPLETES cleanly.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
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

fn git_out(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
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

/// Fake harness: the rat commits a file. The reviewer chains onto that branch
/// and finishes cleanly (its fresh branch forks off the work branch, so its
/// HEAD carries the rat's commit) — the PR decision is driven by the approval
/// gate, not by a reviewer artifact.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Stand up a fresh repo with an `origin` bare remote (the PR push target) and
/// pr-on-approve.cue (harness rewired to fake) shipped into its repo-local
/// workflows dir. Returns (working repo, bare origin). The working repo is
/// intentionally NOT registered with the daemon, so its merge mode resolves to
/// the default Direct — proving `open_pr` opens a PR regardless of policy.
fn init_repo() -> (tempfile::TempDir, tempfile::TempDir) {
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare", "-b", "main"]);

    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    git(
        repo_dir.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    git(repo_dir.path(), &["push", "-u", "origin", "main"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    let wf_src = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("examples")
            .join("workflows")
            .join("pr-on-approve.cue"),
    )
    .unwrap();
    // Drive the control flow with the fake harness instead of real claude/haiku.
    let wf_src = wf_src.replace("\"claude\"", "\"fake\"");
    std::fs::write(wf_dir.join("pr-on-approve.cue"), wf_src).unwrap();
    (repo_dir, origin)
}

/// Start the run and block until it parks at the approval gate. Returns
/// (instance id, the branch ctx is holding = the reviewer's chained branch).
async fn run_to_gate(client: &mut Client, repo: &Path, task: &str) -> (String, String) {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "pr-on-approve",
                "repo": repo.to_string_lossy(),
                "params": {
                    "taskId": task,
                    "description": "Do the risky thing",
                    "implTimeout": "60s",
                    "reviewTimeout": "60s",
                    "approvalTimeout": "60s",
                },
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    // The approval gate is step index 7 (spawn, wait, evaluate, dismiss, spawn,
    // wait, evaluate, gate).
    let mut held_branch = None;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let inst = &status["instance"];
        assert_ne!(
            inst["status"], "failed",
            "run failed before the gate: {}",
            inst["error"]
        );
        if inst["status"] == "running" && inst["current_step"] == 7 {
            held_branch = inst["context"]["active_branch"].as_str().map(String::from);
            break;
        }
    }
    let held_branch = held_branch.expect("workflow never parked at the approval gate");
    (id, held_branch)
}

async fn wait_completed(client: &mut Client, id: &str) {
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => return,
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    panic!("workflow did not complete");
}

/// APPROVE opens a PR for the chained branch: the branch is pushed to origin and
/// left standing, main is NOT advanced (no direct merge, even though the repo is
/// default Direct-mode), and the run completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pr_on_approve_opens_pr_and_keeps_branch() {
    let home = tempfile::tempdir().unwrap();
    let (repo_dir, origin) = init_repo();
    let base_head = git_out(repo_dir.path(), &["rev-parse", "main"])
        .trim()
        .to_string();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let (id, held_branch) = run_to_gate(&mut client, repo_dir.path(), "pr-me").await;
    assert!(
        held_branch.starts_with("rat/"),
        "expected a chained rat branch, got {held_branch}"
    );

    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": true, "by": "operator"}),
        )
        .await
        .unwrap();

    wait_completed(&mut client, &id).await;

    // main is untouched: open_pr never merges, even in a default Direct repo.
    let listing = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        !listing.contains("work-"),
        "open_pr must not merge work onto main: {listing}"
    );
    assert_eq!(
        git_out(repo_dir.path(), &["rev-parse", "main"]).trim(),
        base_head,
        "main must not advance when open_pr opens a PR"
    );

    // The held branch still exists locally (not deleted) ...
    let remaining = git_out(repo_dir.path(), &["branch", "--list", &held_branch]);
    assert!(
        !remaining.trim().is_empty(),
        "open_pr must keep the branch {held_branch}: {remaining}"
    );
    // ... and was pushed to origin (the PR hand-off).
    let remote_ref = Command::new("git")
        .arg("-C")
        .arg(origin.path())
        .args(["rev-parse", "--verify", &format!("refs/heads/{held_branch}")])
        .output()
        .unwrap();
    assert!(
        remote_ref.status.success(),
        "branch {held_branch} must be pushed to origin: {}",
        String::from_utf8_lossy(&remote_ref.stderr)
    );
    // RK_FAKE_HARNESS_CMD is left set: the sibling test shares this process and
    // value, so unsetting here could race its spawns. Harmless to leave.
}

/// REJECT opens no PR: the run still COMPLETES (not failed), nothing is pushed,
/// and main is untouched — the `open_pr` step is only reachable through APPROVE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pr_on_approve_rejection_opens_no_pr() {
    let home = tempfile::tempdir().unwrap();
    let (repo_dir, origin) = init_repo();

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let (id, held_branch) = run_to_gate(&mut client, repo_dir.path(), "reject-me").await;

    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": false, "by": "operator", "reason": "not yet"}),
        )
        .await
        .unwrap();

    wait_completed(&mut client, &id).await;

    // Nothing merged onto main — the open_pr step never ran.
    let listing = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        !listing.contains("work-"),
        "rejected work must not reach main: {listing}"
    );
    // ... and nothing was pushed for the held branch.
    let remote_ref = Command::new("git")
        .arg("-C")
        .arg(origin.path())
        .args(["rev-parse", "--verify", &format!("refs/heads/{held_branch}")])
        .output()
        .unwrap();
    assert!(
        !remote_ref.status.success(),
        "a rejected branch must NOT be pushed to origin: {held_branch}"
    );
    // See the sibling test: RK_FAKE_HARNESS_CMD is intentionally left set.
}
