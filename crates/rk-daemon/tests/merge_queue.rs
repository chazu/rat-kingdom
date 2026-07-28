//! TKT-51: the serialized land / merge queue. Several rats finish on the same
//! base and are dismissed *concurrently*; every branch must land in `main`.
//!
//! Before the per-target merge queue, concurrent auto-merges raced: each merged
//! in a detached worktree captured from the target ref *before* another merge
//! advanced it, so the compare-and-swap in `Repo::merge_branch` bounced the
//! losers to a silent `merged: false` and their branches were left behind — the
//! root cause of the "done ticket never reached main" gap once the fleet started
//! self-driving. The queue makes every land/dismiss into a target take its turn
//! on the freshly-updated tree, so all N merges succeed.
//!
//! The daemon runs on a multi-threaded runtime so the blocking merges can
//! genuinely overlap across worker threads — that's what reproduces the race
//! the queue closes. The assertion (all N merged) is deterministic *with* the
//! queue and flaky *without* it.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
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
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

// Each rat writes a distinct per-agent file (so merges never conflict) and
// reports a clean success.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work by $RK_AGENT for $RK_TASK" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"mq-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"mq-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_dismisses_all_merge_into_main() {
    const N: usize = 5;

    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Spawn N rats, all forked off main.
    let mut names = Vec::with_capacity(N);
    for i in 0..N {
        let spawned = client
            .call(
                "agent.spawn",
                json!({
                    "repo": repo_dir.path().to_string_lossy(),
                    "task": format!("mq-{i}"),
                    "harness": "fake",
                }),
            )
            .await
            .unwrap();
        names.push(spawned["agent"]["name"].as_str().unwrap().to_string());
    }

    // Wait for every rat to finish its commit (registry state, no pane polling).
    for name in &names {
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
    }

    // Fire all N dismisses concurrently, each on its own connection, so the
    // daemon merges them into `main` at the same time — exactly the unattended
    // race the queue must serialize.
    let mut handles = Vec::with_capacity(N);
    for name in names {
        let layout = layout.clone();
        handles.push(tokio::spawn(async move {
            let mut c = Client::connect_as_operator(&layout).await.unwrap();
            c.call("agent.dismiss", json!({"name": name})).await.unwrap()
        }));
    }

    let mut merged = 0usize;
    let mut details = Vec::new();
    for h in handles {
        let res = h.await.unwrap();
        if res["merged"] == true {
            merged += 1;
        }
        details.push(res["detail"].as_str().unwrap_or("").to_string());
    }
    assert_eq!(
        merged, N,
        "every concurrent dismiss must merge into main; details: {details:?}"
    );

    // main carries every rat's work file, and every rat branch was deleted.
    let base = repo_dir.path();
    git(base, &["checkout", "main"]);
    let tracked = git(base, &["ls-files"]);
    let work_files = tracked.lines().filter(|f| f.starts_with("work-")).count();
    assert_eq!(
        work_files, N,
        "all {N} rats' work should be on main, got: {tracked}"
    );
    let branches = git(base, &["branch", "--format=%(refname:short)"]);
    let names: Vec<&str> = branches.lines().map(str::trim).collect();
    assert_eq!(
        names,
        vec!["main"],
        "every merged branch should be deleted, leaving only main"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
