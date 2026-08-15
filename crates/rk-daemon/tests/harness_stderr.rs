//! Harness child stderr used to be discarded at the source
//! (`.stderr(Stdio::null())`), so every reason a starved/misconfigured
//! reviewer produced zero protocol output — rate limit, queueing, auth
//! refresh, model unavailable — was destroyed along with the process. This
//! proves the wiring end to end: stderr lines are tagged in `agent.log`, and a
//! silent (zero-token) death folds its stderr tail into the published result
//! so the cause is visible in `agent.status` and the `agent-failed` inbox row
//! without an operator ever needing `--attach`.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::time::Duration;

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon never came up");
}

fn scratch_repo(dir: &std::path::Path) {
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}");
    };
    git(&["init", "-b", "main"]);
    git(&["config", "user.email", "rat@example.com"]);
    git(&["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "init"]);
}

/// One script for both shapes, branching on `RK_TASK` (mirrors
/// `mid_flight_result.rs`), because `RK_FAKE_HARNESS_CMD` is process-global
/// and the tests in this binary run concurrently — setting it to two
/// different values per test would race.
const FAKE: &str = r#"
read -r _prompt
case "$RK_TASK" in
  narrate)
    echo 'warming up model cache' >&2
    echo '{"type":"system","subtype":"init","session_id":"fake-stderr"}'
    echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"fake-stderr","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
    ;;
  silent-death)
    echo 'auth token refresh failed, retrying' >&2
    echo 'giving up after 3 attempts' >&2
    exit 1
    ;;
esac
"#;

#[tokio::test]
async fn stderr_lines_are_tagged_in_the_agent_log() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "narrate",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "agent never completed");

    let log = client
        .call("agent.log", json!({"name": name}))
        .await
        .unwrap();
    let entries = log["entries"].as_array().unwrap();
    let stderr_entry = entries
        .iter()
        .find(|e| e["kind"] == "stderr")
        .expect("a stderr-kind entry should be in the transcript");
    assert_eq!(stderr_entry["text"], "warming up model cache");
}

/// Writes only to stderr and exits nonzero — no `Started`/`Completed` ever
/// arrives, exactly the "0 tokens" silent reviewer death this exists to
/// diagnose.
#[tokio::test]
async fn silent_death_folds_stderr_into_the_published_result() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "silent-death",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    let mut failed = false;
    let mut status = json!({});
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "failed" {
            failed = true;
            break;
        }
    }
    assert!(
        failed,
        "agent never reached the crashed/failed terminal state"
    );
    assert_eq!(status["agent"]["crashed"], true);
    let result = status["agent"]["result"].as_str().unwrap();
    assert!(
        result.contains("giving up after 3 attempts"),
        "the published result should carry the stderr tail, got: {result}"
    );

    // The same trace is available structurally, not just folded into prose.
    assert!(status["agent"]["stderr_tail"]
        .as_str()
        .unwrap()
        .contains("auth token refresh failed"));

    // And it is what the operator actually sees in the inbox.
    let inbox = client.call("inbox.list", json!({})).await.unwrap();
    let items = inbox["items"].as_array().unwrap();
    let row = items
        .iter()
        .find(|it| it["subject"] == name)
        .expect("agent-failed row for this rat");
    assert_eq!(row["kind"], "agent-failed");
    assert!(row["detail"]
        .as_str()
        .unwrap()
        .contains("giving up after 3 attempts"));
}
