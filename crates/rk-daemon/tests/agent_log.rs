//! `rk log`, end to end through the daemon RPC (TKT-25).
//!
//! The bounded ring, tail, and follow-broadcast are unit-tested in
//! `agent_log`; this proves the wiring at the supervisor boundary: assistant
//! text and tool calls the supervisor used to DROP are now persisted per-agent
//! and served back by `agent.log`.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::time::Duration;

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon never came up");
}

/// A fake rat that narrates: one prose chunk, one tool call, then completes.
/// These `assistant`/`tool_use` events are exactly what `handle_event` used to
/// throw away.
const CHATTY_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"fake-log"}'
echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"planning the gnaw"}]}}'
echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"Bash","id":"t1","input":{}}]}}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"fake-log","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test]
async fn supervisor_persists_transcript_and_log_serves_it() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", CHATTY_FAKE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "gnaw-log",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for completion so all events have been pumped through handle_event.
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

    // The transcript captured both the prose chunk and the tool call.
    let log = client
        .call("agent.log", json!({"name": name}))
        .await
        .unwrap();
    let entries = log["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "text + tool call persisted");
    assert_eq!(entries[0]["kind"], "text");
    assert_eq!(entries[0]["text"], "planning the gnaw");
    assert_eq!(entries[1]["kind"], "tool");
    assert_eq!(entries[1]["name"], "Bash");
    assert!(entries[0]["ts"].is_string(), "each entry is timestamped");

    // `tail` bounds the result to the most-recent entries.
    let tailed = client
        .call("agent.log", json!({"name": name, "tail": 1}))
        .await
        .unwrap();
    let tail = tailed["entries"].as_array().unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0]["kind"], "tool");

    // An unknown agent is an empty transcript, not an error.
    let empty = client
        .call("agent.log", json!({"name": "ghost"}))
        .await
        .unwrap();
    assert!(empty["entries"].as_array().unwrap().is_empty());

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
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
