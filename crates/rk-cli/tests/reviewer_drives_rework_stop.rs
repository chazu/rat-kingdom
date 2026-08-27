//! The STOP route of reviewer-drives-rework: a reviewer STOP verdict must
//! `stop` the workflow (fail the instance), not merge or loop. Kept in its own
//! test binary so its process-global RK_FAKE_HARNESS_CMD never races the
//! happy-path test.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

// Resolved at runtime, not baked in via `env!("CARGO_MANIFEST_DIR")`: a
// shared CARGO_TARGET_DIR can serve this binary unrecompiled to a worktree
// other than the one it was compiled in (TKT-01M0F0GHDPGA24X1TB24A0PZD0).
// Cargo sets a test binary's cwd to its package's manifest directory on
// every run, so walking up from `current_dir()` is always correct for the
// current process.
fn workspace_root() -> std::path::PathBuf {
    let cwd = std::env::current_dir().expect("test process must have a current directory");
    cwd.ancestors()
        .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("crates").is_dir())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not find the rat-kingdom workspace above runtime directory {}",
                cwd.display()
            )
        })
}

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

async fn connect(layout: &Layout) -> Client {
    // Match the normal daemon-backed integration-test startup budget.
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Fake harness: the reviewer always votes STOP; rats commit.
fn fake_harness(rk_bin: &str) -> String {
    format!(
        r#"
read -r _prompt
echo '{{"type":"system","subtype":"init","session_id":"wf-fake"}}'
if [ "$RK_ROLE" = "reviewer" ]; then
    "{rk_bin}" out artifact "$RK_REPO" review --payload "{{\"task\":\"$RK_TASK\",\"recommendation\":\"STOP\"}}" >/dev/null 2>&1
else
    echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
    git add . >/dev/null 2>&1
    git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
fi
# Both roles declare themselves finished before the turn ends, as a real primed
# rat does. A generation that reaches the exit-flush without a `task_done`
# publishes as a failure (TKT-175), and the workflow gates on `is_error: false`
# before it ever reads the reviewer's STOP.
"{rk_bin}" done "did the work" >/dev/null 2>&1
echo '{{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#
    )
}

#[tokio::test]
async fn reviewer_stop_verdict_aborts_the_run() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(repo_dir.path().join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    let wf_src = std::fs::read_to_string(
        workspace_root()
            .join("examples")
            .join("workflows")
            .join("reviewer-drives-rework.cue"),
    )
    .unwrap();
    let wf_src = wf_src.replace("\"claude\"", "\"fake\"");
    std::fs::write(wf_dir.join("reviewer-drives-rework.cue"), wf_src).unwrap();

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fake_harness(env!("CARGO_BIN_EXE_rk")),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": "reviewer-drives-rework-stop", "path": repo_dir.path()}),
        )
        .await
        .unwrap();

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "reviewer-drives-rework",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {
                    "taskId": "fix-login",
                    "workTimeout": "60s",
                    "reviewTimeout": "60s",
                    // See reviewer_drives_rework.rs for why this needs its
                    // own generous budget separate from reviewTimeout: it
                    // covers the reviewer's `rk out artifact` subprocess
                    // landing and being read back, a second process spawn +
                    // daemon round trip after the reviewer's harness turn
                    // already completed (TKT-01M0GBC0PK2M52QGB0A4H0PM1F).
                    "readTimeout": "5m",
                    "maxRounds": 3,
                },
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    // One review round still carries its own 60s workTimeout/reviewTimeout
    // plus a 5m `readTimeout` override on the verdict read
    // (examples/workflows/reviewer-drives-rework.cue) — internal ceilings the
    // workflow is allowed to take: 60s(work) + 60s(review) + 5m(read) = 420s
    // = 7m theoretical worst case if every internal ceiling maxed out (which
    // would itself flip the instance to `failed` and be caught below
    // immediately). The outer window must comfortably EXCEED that sum, not
    // stay under it as the previous fix did
    // (TKT-01M0GBC0PK2M52QGB0A4H0PM1F) — this loop must tolerate a workflow
    // that legitimately needs close to its full, now-larger internal budget
    // to finish under cargo-test-workspace-wide CPU/disk contention
    // (TKT-01M0D2APS09AXKB4AHAYHCPSPX). 12 minutes gives real headroom above
    // the 7m theoretical max.
    let mut error: Option<String> = None;
    for _ in 0..7200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "failed" => {
                error = Some(status["instance"]["error"].as_str().unwrap_or("").into());
                break;
            }
            "completed" => panic!("STOP verdict should fail the run, not complete"),
            _ => {}
        }
    }
    let error = error.expect("workflow did not finish");
    assert!(
        error.contains("workflow stopped") && error.contains("STOP"),
        "failure should carry the stop reason: {error}"
    );

    // Only one review round ran — STOP aborts immediately, it does not loop.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let reviewers = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["role"] == "reviewer")
        .count();
    assert_eq!(
        reviewers, 1,
        "STOP must not spawn a second review: {agents}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
