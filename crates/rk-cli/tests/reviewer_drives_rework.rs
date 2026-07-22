//! End-to-end proof of the reviewer-drives-rework control flow: the new
//! `read` / `when` / `repeat` / `break` / `stop` primitives running through the
//! real daemon + supervisor against the fake harness.
//!
//! The fake harness branches on `$RK_ROLE`: rats make a commit; the reviewer
//! records a real verdict *artifact tuple* by shelling out to the `rk` binary
//! (reachable because the supervisor sets `RK_HOME` in the agent env). The
//! reviewer says REWORK on its first round and APPROVE on the second, so the
//! loop runs review -> rework -> re-review -> merge, exercising every new step.

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

const RESULT_LINE: &str = r#"echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#;

/// Fake harness: reviewer emits a verdict artifact (REWORK then APPROVE across
/// rounds, tracked by a counter file under RK_HOME); everyone else commits.
fn fake_harness(rk_bin: &str) -> String {
    format!(
        r#"
read -r _prompt
echo '{{"type":"system","subtype":"init","session_id":"wf-fake"}}'
if [ "$RK_ROLE" = "reviewer" ]; then
    COUNT_FILE="$RK_HOME/review-count"
    n=$(cat "$COUNT_FILE" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > "$COUNT_FILE"
    if [ "$n" -eq 1 ]; then rec="REWORK"; else rec="APPROVE"; fi
    "{rk_bin}" out artifact "$RK_REPO" review --payload "{{\"task\":\"$RK_TASK\",\"recommendation\":\"$rec\"}}" >/dev/null 2>&1
else
    echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
    git add . >/dev/null 2>&1
    git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
fi
{RESULT_LINE}
"#
    )
}

#[tokio::test]
async fn reviewer_drives_rework_loops_then_merges() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    // Ship the workflow into the repo-local workflows dir.
    let wf_dir = repo_dir.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    let wf_src = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("examples")
            .join("workflows")
            .join("reviewer-drives-rework.cue"),
    )
    .unwrap();
    // Drive the control flow with the fake harness instead of real claude.
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

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "reviewer-drives-rework",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {
                    "taskId": "fix-login",
                    "description": "Fix the login redirect loop",
                    "workTimeout": "60s",
                    "reviewTimeout": "60s",
                    "maxRounds": 3,
                },
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            "completed" => {
                completed = true;
                break;
            }
            "failed" => panic!("workflow failed: {}", status["instance"]["error"]),
            _ => {}
        }
    }
    if !completed {
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let agents = client.call("agent.list", json!({})).await.unwrap();
        let arts = client
            .call("space.scan", json!({"category": "artifact"}))
            .await
            .unwrap();
        panic!("workflow did not complete\nstatus={status}\nagents={agents}\nartifacts={arts}");
    }

    // Two review rounds ran (REWORK routed back, then APPROVE broke the loop):
    // rat, reviewer #1, rework rat, reviewer #2. Exactly two reviewers proves
    // read -> when -> repeat looped once and `break` stopped it on APPROVE.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let list = agents["agents"].as_array().unwrap();
    let reviewers = list.iter().filter(|a| a["role"] == "reviewer").count();
    assert_eq!(reviewers, 2, "expected exactly two review rounds: {agents}");
    let reworked = list
        .iter()
        .any(|a| a["task"].as_str().unwrap_or("").starts_with("rework-"));
    assert!(
        reworked,
        "the REWORK verdict should have spawned a rework rat"
    );

    // The rework chained onto the rat's work: both commits exist, and the
    // rework builds on the original fix (the approved chain-tip branch holds
    // everything for the orchestrator to merge).
    let logs = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["log", "--all", "--oneline"])
        .output()
        .unwrap();
    let log = String::from_utf8_lossy(&logs.stdout).to_string();
    assert!(
        log.contains("work: fix-login") && log.contains("work: rework-fix-login"),
        "both the fix and the rework commits should exist: {log}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
