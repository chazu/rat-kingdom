//! End-to-end proof of the gated-merge reject/approve routing, exercising the
//! real `examples/workflows/gated-merge.cue` through the daemon + supervisor
//! against the fake harness.
//!
//! The workflow parks at a human approval gate, then `read`/`when`-routes the
//! decision: APPROVE merges the branch; REJECT tears the worktree down cleanly
//! but leaves the branch UNMERGED — and, crucially, the run COMPLETES rather
//! than failing. This is the behavior TKT-5 rebuilt on the control-flow
//! primitives, replacing the old evaluate-failure that left a failed instance.

mod fixture;

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
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Fake harness: the rat commits a file; there is no reviewer in this workflow.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Stand up a fresh repo with gated-merge.cue (harness rewired to fake) shipped
/// into its repo-local workflows dir. Returns the initialized repo tempdir.
fn init_repo() -> tempfile::TempDir {
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    let wf_src = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("examples")
            .join("workflows")
            .join("gated-merge.cue"),
    )
    .unwrap();
    // Drive the control flow with the fake harness instead of real claude.
    let wf_src = wf_src.replace("\"claude\"", "\"fake\"");
    std::fs::write(wf_dir.join("gated-merge.cue"), wf_src).unwrap();
    repo_dir
}

/// Start the run and block until it parks at the approval gate (the `gate` step,
/// index 3). Returns the instance id.
async fn run_to_gate(client: &mut Client, repo: &Path, task: &str) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "gated-merge",
                "repo": repo.to_string_lossy(),
                "params": {
                    "taskId": task,
                    "description": "Do the risky thing",
                    "implTimeout": "60s",
                    "approvalTimeout": "60s",
                },
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut parked = false;
    for _ in 0..200 {
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
        if inst["status"] == "running" && inst["current_step"] == 3 {
            parked = true;
            break;
        }
    }
    assert!(parked, "workflow never parked at the approval gate");
    id
}

async fn wait_completed(client: &mut Client, id: &str) {
    for _ in 0..200 {
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

/// APPROVE routes to a merging dismiss: the rat's work lands on main and the run
/// completes.
#[tokio::test]
async fn gated_merge_approval_merges_the_branch() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = init_repo();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let id = run_to_gate(&mut client, repo_dir.path(), "approve-me").await;

    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": true, "by": "operator"}),
        )
        .await
        .unwrap();

    wait_completed(&mut client, &id).await;

    // The approved work reached main, and its branch was consumed by the merge.
    let listing = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(listing.contains("work-"), "approved work in main: {listing}");
    let branches = git_out(repo_dir.path(), &["branch", "--list", "rat/*"]);
    assert!(
        branches.trim().is_empty(),
        "merged branch should be deleted: {branches}"
    );
    // RK_FAKE_HARNESS_CMD is left set: the sibling test in this binary runs in
    // parallel and shares the same value, so unsetting it here could race its
    // spawns. It is scoped to this test process and harmless to leave.
}

/// REJECT routes to a --no-merge dismiss: the run still COMPLETES (not failed),
/// the branch is preserved, and nothing reaches main. This is the heart of
/// TKT-5 — a veto ends the run cleanly instead of as a failed instance.
#[tokio::test]
async fn gated_merge_rejection_ends_cleanly_unmerged() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = init_repo();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let id = run_to_gate(&mut client, repo_dir.path(), "reject-me").await;

    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": false, "by": "operator", "reason": "not yet"}),
        )
        .await
        .unwrap();

    // The rejected run COMPLETES cleanly — the whole point of the rewrite.
    wait_completed(&mut client, &id).await;

    // Nothing reached main...
    let listing = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        !listing.contains("work-"),
        "rejected work must not merge: {listing}"
    );
    // ...but the branch (with its commit) is preserved for review — dismiss
    // --no-merge tore down the worktree without deleting the branch.
    let branches = git_out(repo_dir.path(), &["branch", "--list", "rat/*"]);
    assert!(
        !branches.trim().is_empty(),
        "rejected branch should be preserved unmerged: {branches}"
    );
    let log = git_out(repo_dir.path(), &["log", "--all", "--oneline"]);
    assert!(
        log.contains("work: reject-me"),
        "the rejected commit should still exist on its branch: {log}"
    );
    // See the sibling test: RK_FAKE_HARNESS_CMD is intentionally left set.
}
