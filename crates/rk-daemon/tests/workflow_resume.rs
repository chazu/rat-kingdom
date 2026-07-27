//! End-to-end proof of workflow instance restart recovery (TKT-52).
//!
//! A running instance parked at a human approval gate is persisted to disk,
//! survives a full daemon stop/start, is rehydrated AND resumed by the new
//! daemon (re-parking at the same gate from its persisted step cursor), and can
//! then be approved to completion — merging the branch the pre-restart rat
//! produced. Before this ticket the new daemon dropped the in-flight instance
//! on the floor: it never loaded the persisted state back, so a crash/restart
//! mid-run silently lost the workflow.
//!
//! The two daemons share one `home` (so `agents.json` and
//! `workflow-instances/*.json` carry across the restart) but each opens its own
//! in-memory tuplespace — exactly the state a fresh process starts with. The
//! approval therefore has to be delivered to the *second* daemon, proving the
//! resumed gate is genuinely live again rather than replaying a stale decision.

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
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Fake harness: the rat commits a file and reports success. No reviewer.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Stand up a fresh repo with gated-merge.cue (harness rewired to fake) in its
/// repo-local workflows dir.
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
    let wf_src = wf_src.replace("\"claude\"", "\"fake\"");
    std::fs::write(wf_dir.join("gated-merge.cue"), wf_src).unwrap();
    repo_dir
}

/// Launch gated-merge and block until it parks at the approval gate (step 3).
/// `approvalTimeout` is deliberately long so the first daemon's now-orphaned
/// gate task never fires a fail-closed decision during the test window.
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
                    "approvalTimeout": "600s",
                },
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

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
            return id;
        }
    }
    panic!("workflow never parked at the approval gate");
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

/// Wait for a stopped daemon to release its socket so a successor can bind.
async fn wait_socket_gone(layout: &Layout) {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !layout.socket_path().exists() {
            return;
        }
    }
    panic!("daemon did not release its socket on shutdown");
}

#[tokio::test]
async fn parked_gate_instance_survives_restart_and_completes() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = init_repo();
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());

    // --- Daemon A: run to the approval gate, then stop mid-run. ---
    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    let id = run_to_gate(&mut client, repo_dir.path(), "restart-me").await;

    // The parked instance was persisted to disk — the state the successor reads.
    let inst_file = home
        .path()
        .join("workflow-instances")
        .join(format!("{id}.json"));
    assert!(inst_file.exists(), "parked instance should be persisted");

    // Stop daemon A and wait for it to release the socket + task.
    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    // --- Daemon B: fresh tuplespace, same home. Rehydrate must resume. ---
    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The resumed instance is back in the registry, still Running and re-parked
    // at the approval gate (step 3) — rehydrate both LOADED and RESUMED it.
    let mut reparked = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let inst = &status["instance"];
        assert_ne!(inst["status"], "failed", "resume failed: {}", inst["error"]);
        if inst["status"] == "running" && inst["current_step"] == 3 {
            reparked = true;
            break;
        }
    }
    assert!(
        reparked,
        "instance did not resume + re-park at the gate after restart"
    );

    // Approve via the NEW daemon: the gate is genuinely live again. The resumed
    // run drives the merge to completion.
    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": true, "by": "operator"}),
        )
        .await
        .unwrap();
    wait_completed(&mut client, &id).await;

    // The pre-restart rat's work reached main: nothing was lost across the
    // restart, and the resumed workflow completed the human-gated merge.
    let listing = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        listing.contains("work-"),
        "approved work should be in main after restart+resume: {listing}"
    );
}
