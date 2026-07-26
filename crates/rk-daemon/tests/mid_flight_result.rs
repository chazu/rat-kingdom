//! TKT-160 regression: an agent generation publishes exactly ONE durable
//! `harness_result`, and it is the turn the agent actually finished on.
//!
//! A harness returns control once per TURN, not once per task. A background
//! test suite finishing, a re-armed monitor, or a task notification each end a
//! turn and produce a `Completed` event while the rat is still mid-task, and
//! nothing in the payload distinguishes one from a real finish (`is_error` is
//! `false` on both). Twelve agent generations in the live fleet emitted more
//! than one `harness_result` this way. Because a `LIMIT 1` read returns the
//! OLDEST match, every reader keyed on the agent name got the MID-FLIGHT turn:
//! a workflow `wait` unblocked on "the full cargo test pass is still running",
//! the `evaluate` behind it judged that text, and three steward reviewers whose
//! APPROVE/REWORK landed in a later turn were read as having no verdict at all.
//! The TKT-146 generation floor does not help — these are turns WITHIN one
//! generation, milliseconds apart.
//!
//! The gate is in the supervisor rather than in the workflow `wait`, because
//! `wait` is not the only reader: the reactor's steward trigger and the ticket
//! auto-close consume the same event. Three proofs let a turn through, one per
//! test here: the rat ran `rk done`; the process exited; or the turn failed.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
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

/// One script for all three shapes, branching on `RK_TASK`, because
/// `RK_FAKE_HARNESS_CMD` is process-global and the tests in this binary run
/// concurrently.
///
/// `multi-turn` is the shape that matters: it reports a mid-flight turn, then
/// STAYS ALIVE waiting for the next input exactly as a real Claude session does
/// between turns, then reports its real result and *still* does not exit. So
/// nothing about the process proves the second result is the last one — only
/// the `task_done` the test writes between the two turns.
const FAKE: &str = r#"
result() { printf '{"type":"result","subtype":"success","is_error":%s,"result":"%s","session_id":"tkt160","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}\n' "$1" "$2"; }
echo '{"type":"system","subtype":"init","session_id":"tkt160"}'
read -r _prompt
case "$RK_TASK" in
  multi-turn)
    result false "Tests still compiling/running - I will wait for the monitor"
    read -r _steer
    result false "FINAL: all green, APPROVE"
    sleep 60
    ;;
  exits-after-one-turn)
    result false "did the work"
    ;;
  fails)
    result true "harness error: max turns exceeded"
    sleep 60
    ;;
esac
"#;

fn init_repo(dir: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    dir.file_name().unwrap().to_string_lossy().to_string()
}

async fn start(task: &str) -> (tempfile::TempDir, tempfile::TempDir, Client, String, String) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": task,
                "role": "rat",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    (home, repo_dir, client, repo_name, agent)
}

/// Every `harness_result` published for `agent`, oldest first.
async fn results(client: &mut Client, repo: &str, agent: &str) -> Vec<Value> {
    client
        .call(
            "space.scan",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "harness_result",
                "payload_search": format!("\"agent\":\"{agent}\""),
            }),
        )
        .await
        .unwrap()["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["payload"].clone())
        .collect()
}

/// Poll `agent.status` until the record's `result` contains `needle`.
async fn await_recorded_result(client: &mut Client, agent: &str, needle: &str) {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap();
        if status["agent"]["result"]
            .as_str()
            .is_some_and(|r| r.contains(needle))
        {
            return;
        }
    }
    panic!("agent {agent} never recorded a result containing {needle:?}");
}

/// The heart of TKT-160: a turn that ends while the rat is still working must
/// not be published as that rat's completion. Only the turn it ran `rk done`
/// on is.
#[tokio::test]
async fn a_mid_flight_turn_is_not_published_as_the_completion() {
    let (_home, _repo_dir, mut client, repo, agent) = start("multi-turn").await;

    // Turn 1 is fully processed — the record carries its text — yet nothing is
    // published, because nothing yet says the rat is finished.
    await_recorded_result(&mut client, &agent, "still compiling").await;
    let published = results(&mut client, &repo, &agent).await;
    assert!(
        published.is_empty(),
        "a mid-flight turn was published as a completion: {published:?}"
    );

    // The rat runs `rk done`, then finishes its turn. `task_done` is written
    // once per generation by the rat itself, so it — unlike the harness's
    // per-turn result — cannot duplicate.
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "task_done",
                "payload": {"agent": agent, "task": "multi-turn", "summary": "done"},
            }),
        )
        .await
        .unwrap();
    client
        .call("agent.steer", json!({"name": agent, "message": "carry on"}))
        .await
        .unwrap();

    await_recorded_result(&mut client, &agent, "FINAL").await;
    let mut published = Vec::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        published = results(&mut client, &repo, &agent).await;
        if !published.is_empty() {
            break;
        }
    }
    assert_eq!(
        published.len(),
        1,
        "exactly one completion per generation: {published:?}"
    );
    // The process is still alive (`sleep 60`), so this publication was driven
    // by the rat's `rk done` and not by its exit.
    let result = published[0]["result"].as_str().unwrap_or("");
    assert!(
        result.contains("FINAL"),
        "the completion should carry the finishing turn's result, got: {result:?}"
    );
    assert_eq!(published[0]["is_error"], json!(false));
}

/// The second proof that no further turn can follow: the process is gone.
/// Harnesses that end with the run (codex, axe, the test fake) take this path
/// for every agent, so a rat that never got to its `rk done` still reports.
#[tokio::test]
async fn an_agent_that_exits_still_publishes_its_last_turn() {
    let (_home, _repo_dir, mut client, repo, agent) = start("exits-after-one-turn").await;

    let mut published = Vec::new();
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        published = results(&mut client, &repo, &agent).await;
        if !published.is_empty() {
            break;
        }
    }
    assert_eq!(published.len(), 1, "expected one completion: {published:?}");
    assert!(published[0]["result"]
        .as_str()
        .unwrap_or("")
        .contains("did the work"));
}

/// The third proof: a failed turn ended the session, so there is no later turn
/// to prefer. Holding it back would turn a fast, legible failure into a `wait`
/// timeout — so it publishes immediately, with the process still running.
#[tokio::test]
async fn a_failed_turn_publishes_immediately() {
    let (_home, _repo_dir, mut client, repo, agent) = start("fails").await;

    let mut published = Vec::new();
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        published = results(&mut client, &repo, &agent).await;
        if !published.is_empty() {
            break;
        }
    }
    assert_eq!(published.len(), 1, "expected one completion: {published:?}");
    assert_eq!(published[0]["is_error"], json!(true));
}
