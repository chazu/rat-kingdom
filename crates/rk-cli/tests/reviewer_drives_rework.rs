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
    // Workspace-wide test execution can start several daemon-backed binaries at
    // once; allow thirty seconds for the socket rather than making startup
    // depend on a short scheduler window.
    //
    // Connect as the operator explicitly (TKT-182): this test drives
    // `workflow.run`, which is operator-only, and a rat's spawn env sets
    // `RK_AGENT`, which test processes inherit. Reading identity from the
    // ambient environment made this test fail inside every rat and pass only
    // in an operator shell.
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
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
# Both roles declare themselves finished before the turn ends, as a real primed
# rat does. A generation that reaches the exit-flush without a `task_done`
# publishes as a failure (TKT-175), and every `evaluate` in this workflow gates
# on `is_error: false`.
"{rk_bin}" done "did the work" >/dev/null 2>&1
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
                    // The workflow's `read` step (examples/workflows/
                    // reviewer-drives-rework.cue) has its own internal
                    // timeout, separate from workTimeout/reviewTimeout above:
                    // it waits for the reviewer's `rk out artifact` subprocess
                    // to land and be read back, which is a second process
                    // spawn + daemon round trip after the reviewer's harness
                    // turn already completed. Under cargo-test-workspace-wide
                    // contention that hop can itself take a while even though
                    // the reviewer generation finished promptly, so it needs
                    // its own generous budget rather than inheriting
                    // reviewTimeout's. (TKT-01M0GBC0PK2M52QGB0A4H0PM1F: the
                    // previous fix only widened the outer poll loop below,
                    // which cannot help — this internal read step returns a
                    // hard `failed` well inside any outer window once ITS OWN
                    // ceiling is hit.)
                    "readTimeout": "5m",
                    "maxRounds": 3,
                },
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    // This run crosses two full review rounds, each carrying its own 60s
    // workTimeout/reviewTimeout plus a 5m `readTimeout` override on the
    // verdict read (examples/workflows/reviewer-drives-rework.cue) — those
    // are the *internal* ceilings the workflow itself is allowed to take.
    // Sequence: initial work(60s) -> round1 review(60s)+read(5m)+rework
    // work(60s) -> round2 review(60s)+read(5m) = 780s = 13m theoretical worst
    // case if every internal ceiling maxed out (which would itself flip the
    // instance to `failed` and be caught below immediately). The outer
    // window must comfortably EXCEED that sum, not stay under it as the
    // previous fix did (TKT-01M0GBC0PK2M52QGB0A4H0PM1F) — unlike a step that
    // fails outright at its ceiling, this loop must tolerate a workflow that
    // legitimately needs close to its full, now-larger internal budget to
    // succeed under cargo-test-workspace-wide CPU/disk contention
    // (TKT-01M0D2APS09AXKB4AHAYHCPSPX). 20 minutes gives real headroom above
    // the 13m theoretical max.
    let mut completed = false;
    for _ in 0..12000 {
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
