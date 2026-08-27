//! End-to-end proof of the `land` step (TKT-11), exercising the real
//! `examples/workflows/land-on-approve.cue` through the daemon + supervisor
//! against the fake harness.
//!
//! The workflow's reviewer is chained off the rat's work branch — so the
//! reviewer's own base is that work branch, NOT main. A plain `dismiss` could
//! therefore only ever merge into the work branch; it could never land the work
//! on main. The workflow parks at a human approval gate, and on APPROVE a
//! `land` step names {branch: {{ctx.activeBranch}}, target: main} explicitly and
//! merges the reviewed branch straight onto main — the manual final-merge hop
//! the reviewer could not close on its own. On REJECT nothing lands and the run
//! still COMPLETES cleanly.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

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

/// Fake harness: the rat commits a file. The reviewer chains onto that branch
/// and simply finishes cleanly (it forks a fresh branch off the work branch, so
/// its HEAD carries the rat's commit) — the merge decision is driven by the
/// approval gate, not by a reviewer artifact.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Stand up a fresh repo with land-on-approve.cue (harness rewired to fake)
/// shipped into its repo-local workflows dir.
fn init_repo() -> tempfile::TempDir {
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo_dir.path());

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    let wf_src = std::fs::read_to_string(
        support::workspace_root()
            .join("examples")
            .join("workflows")
            .join("land-on-approve.cue"),
    )
    .unwrap();
    // Drive the control flow with the fake harness instead of real claude/haiku.
    let wf_src = wf_src.replace("\"claude\"", "\"fake\"");
    std::fs::write(wf_dir.join("land-on-approve.cue"), wf_src).unwrap();
    repo_dir
}

/// Start the run and block until it parks at the approval gate. Returns
/// (instance id, the branch ctx is holding = the reviewer's chained branch).
async fn run_to_gate(client: &mut Client, repo: &Path, task: &str) -> (String, String) {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "land-on-approve",
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

/// APPROVE lands the chained branch directly onto main — the merge a plain
/// dismiss (into the reviewer's work-branch base) could never do — and deletes
/// the landed branch. The run completes.
#[tokio::test]
async fn land_on_approve_merges_chained_branch_to_main() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = init_repo();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let (id, held_branch) = run_to_gate(&mut client, repo_dir.path(), "land-me").await;
    // The held branch is the reviewer's, chained off the work branch — its base
    // is that work branch, so only an explicit `land` can put it on main.
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

    // The reviewed work reached main via the land step's explicit merge...
    let listing = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        listing.contains("work-"),
        "landed work must be on main: {listing}"
    );
    let log = git_out(repo_dir.path(), &["log", "--oneline", "main"]);
    assert!(
        log.contains("into main"),
        "main should carry the land merge commit: {log}"
    );
    // ...and the landed branch was deleted by the successful merge.
    let remaining = git_out(repo_dir.path(), &["branch", "--list", &held_branch]);
    assert!(
        remaining.trim().is_empty(),
        "landed branch {held_branch} should be deleted: {remaining}"
    );
    // RK_FAKE_HARNESS_CMD is left set: the sibling test shares this process and
    // value, so unsetting here could race its spawns. Harmless to leave.
}

/// REJECT lands nothing: the run still COMPLETES (not failed), and no work
/// reaches main — the `land` step is only reachable through the APPROVE branch.
#[tokio::test]
async fn land_on_approve_rejection_lands_nothing() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = init_repo();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let (id, _held) = run_to_gate(&mut client, repo_dir.path(), "reject-me").await;

    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": false, "by": "operator", "reason": "not yet"}),
        )
        .await
        .unwrap();

    wait_completed(&mut client, &id).await;

    // Nothing landed on main — the land step never ran.
    let listing = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        !listing.contains("work-"),
        "rejected work must not land on main: {listing}"
    );
    // See the sibling test: RK_FAKE_HARNESS_CMD is intentionally left set.
}
