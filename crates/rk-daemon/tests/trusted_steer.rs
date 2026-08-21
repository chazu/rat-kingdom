//! RPC-to-harness proof for trusted Codex steering.
//!
//! The fake Codex process speaks only the small JSONL subset the adapter needs:
//! it holds the initial turn, accepts SIGINT, and either starts or rejects the
//! resumed turn. The test observes the real daemon tuples, not adapter-only
//! events, so generation provenance and the acknowledgement boundary are both
//! exercised over RPC.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;
use support::connect;

static ENV_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn env_lock() -> tokio::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn git(dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.test"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# trusted steer\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn install_codex(dir: &Path) {
    let binary = dir.join("codex");
    std::fs::write(
        &binary,
        r#"#!/bin/sh
if [ "$2" = "resume" ]; then
  echo '{"type":"thread.started","thread_id":"resumed-session"}'
  echo '{"type":"item.completed","item":{"item_type":"command_execution","command":"echo rk_control message_id=lookalike"}}'
  echo '{"type":"item.completed","item":{"item_type":"agent_message","text":"control applied"}}'
  exit 0
fi
echo '{"type":"thread.started","thread_id":"initial-session"}'
trap 'exit 130' INT
while :; do sleep 1; done
"#,
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn install_replay_codex(dir: &Path, mode: &Path, log: &Path) {
    let binary = dir.join("codex");
    let script = format!(
        r#"#!/bin/sh
if [ "$2" = "resume" ]; then
  case "$*" in
    *"RK TRUSTED CONTROL TURN"*)
      echo "control" >> "{log}"
      if [ "$(cat "{mode}")" = "fail" ]; then
        echo '{{"type":"error","message":"session rejected"}}'
        exit 2
      fi
      echo '{{"type":"thread.started","thread_id":"control-session"}}'
      echo '{{"type":"item.completed","item":{{"item_type":"agent_message","text":"control applied"}}}}'
      exit 0
      ;;
    *)
      echo '{{"type":"thread.started","thread_id":"respawn-session"}}'
      trap 'exit 130' INT
      while :; do sleep 1; done
      ;;
  esac
fi
echo '{{"type":"thread.started","thread_id":"initial-session"}}'
trap 'exit 130' INT
while :; do sleep 1; done
"#,
        mode = mode.display(),
        log = log.display(),
    );
    std::fs::write(&binary, script).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
}

async fn wait_for_socket_gone(layout: &Layout) {
    for _ in 0..200 {
        if !layout.socket_path().exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "daemon socket did not disappear: {}",
        layout.socket_path().display()
    );
}

async fn wait_for_tuple(
    client: &mut Client,
    params: Value,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    for _ in 0..200 {
        let result = client.call("space.scan", params.clone()).await.unwrap();
        if let Some(tuple) = result["tuples"]
            .as_array()
            .and_then(|tuples| tuples.iter().find(|tuple| predicate(&tuple["payload"])))
        {
            return tuple["payload"].clone();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("tuple did not arrive: {params}");
}

#[tokio::test]
async fn rpc_steer_persists_live_generation_and_ignores_tool_lookalikes() {
    let _env_lock = env_lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let bin = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    install_codex(bin.path());

    let old_path = std::env::var_os("PATH");
    let mut path = bin.path().as_os_str().to_os_string();
    if let Some(old_path) = &old_path {
        path.push(":");
        path.push(old_path);
    }
    std::env::set_var("PATH", path);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _daemon = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo.path().to_string_lossy(),
                "task": "trusted-steer",
                "harness": "codex",
            }),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let repo_name = spawned["agent"]["repo_name"].as_str().unwrap().to_string();

    let response = client
        .call(
            "agent.steer",
            json!({"name": agent, "message": "continue with the focused tests"}),
        )
        .await
        .unwrap();
    let delivery_generation = response["delivery_generation"].as_str().unwrap();
    let resume_generation = response["resume_generation"].as_str().unwrap();
    assert_eq!(delivery_generation, resume_generation);
    assert_ne!(
        delivery_generation,
        spawned["agent"]["created_at"].as_str().unwrap()
    );
    assert_ne!(
        delivery_generation,
        spawned["agent"]["spawn"].as_str().unwrap()
    );

    let message = wait_for_tuple(
        &mut client,
        json!({"category":"message", "scope":repo_name, "identity":agent}),
        |payload| payload["message_id"] == response["message_id"],
    )
    .await;
    assert_eq!(
        message["delivery_generation"],
        response["delivery_generation"]
    );
    assert_eq!(message["resume_generation"], response["resume_generation"]);

    let ack = wait_for_tuple(
        &mut client,
        json!({"category":"event", "scope":repo_name, "identity":"rk_control_ack"}),
        |payload| payload["message_id"] == response["message_id"],
    )
    .await;
    assert_eq!(ack["delivery_generation"], response["delivery_generation"]);
    assert_eq!(ack["resume_generation"], response["resume_generation"]);
    assert_eq!(ack["acknowledged"], true);

    // The Codex fixture emitted a command-output lookalike, but the daemon
    // only acknowledges the typed ControlDelivered event from the adapter.
    let all_acks = client
        .call(
            "space.scan",
            json!({"category":"event", "scope":repo_name, "identity":"rk_control_ack"}),
        )
        .await
        .unwrap();
    assert_eq!(all_acks["tuples"].as_array().unwrap().len(), 1);

    let _ = client.call("stop", json!({})).await;
    if let Some(old_path) = old_path {
        std::env::set_var("PATH", old_path);
    }
}

#[tokio::test]
async fn unacknowledged_control_replays_once_after_restart_and_never_after_ack() {
    let _env_lock = env_lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let bin = tempfile::tempdir().unwrap();
    let mode = tempfile::NamedTempFile::new().unwrap();
    let log = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(mode.path(), "fail\n").unwrap();
    init_repo(repo.path());
    install_replay_codex(bin.path(), mode.path(), log.path());

    let old_path = std::env::var_os("PATH");
    let mut path = bin.path().as_os_str().to_os_string();
    if let Some(old_path) = &old_path {
        path.push(":");
        path.push(old_path);
    }
    std::env::set_var("PATH", path);

    let layout = Layout::at(home.path());
    let config = rk_core::config::Config::default();
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let _daemon_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo.path().to_string_lossy(),
                "task": "trusted-steer-replay",
                "harness": "codex",
            }),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let repo_name = spawned["agent"]["repo_name"].as_str().unwrap().to_string();
    let first = client
        .call(
            "agent.steer",
            json!({"name": agent, "message": "resume after the interruption"}),
        )
        .await
        .unwrap();
    let original_resume_generation = first["resume_generation"].as_str().unwrap().to_string();

    // The first resume process rejects before Started, so no ack exists and
    // the durable message must remain eligible for replay.
    for _ in 0..200 {
        let status = client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap();
        if status["agent"]["state"] == "failed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let acks_before = client
        .call(
            "space.scan",
            json!({"category":"event", "scope":repo_name, "identity":"rk_control_ack"}),
        )
        .await
        .unwrap();
    assert!(acks_before["tuples"].as_array().unwrap().is_empty());

    client.call("stop", json!({})).await.ok();
    wait_for_socket_gone(&layout).await;

    // A fresh daemon opens the durable tuplespace and respawns the failed
    // record. `track_session` is the restart boundary that replays the
    // unacknowledged message onto the new live session.
    std::fs::write(mode.path(), "ok\n").unwrap();
    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let _daemon_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;
    client
        .call("agent.respawn", json!({"name": agent}))
        .await
        .unwrap();
    let ack = wait_for_tuple(
        &mut client,
        json!({"category":"event", "scope":repo_name, "identity":"rk_control_ack"}),
        |payload| payload["message_id"] == first["message_id"],
    )
    .await;
    assert_eq!(ack["delivery_generation"], first["delivery_generation"]);
    assert_ne!(
        ack["resume_generation"].as_str().unwrap(),
        original_resume_generation
    );
    let controls_before = std::fs::read_to_string(log.path()).unwrap().lines().count();
    assert_eq!(controls_before, 2, "initial failure plus one replay");

    // A second launch after the acknowledgement must not replay the same
    // message. The fixture logs only trusted control turns, so the count is a
    // direct harness-side at-most-once proof.
    client
        .call("agent.respawn", json!({"name": agent}))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let controls = std::fs::read_to_string(log.path()).unwrap().lines().count();
    assert_eq!(
        controls,
        controls_before,
        "acknowledged control must not replay: log={:?}",
        std::fs::read_to_string(log.path()).unwrap()
    );

    let _ = client.call("stop", json!({})).await;
    if let Some(old_path) = old_path {
        std::env::set_var("PATH", old_path);
    }
}
