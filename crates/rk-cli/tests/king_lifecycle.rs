use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn install_fake_herdr(root: &Path) -> (PathBuf, PathBuf) {
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let script = bin.join("herdr");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$RK_TEST_HERDR_LOG"

case "$1 $2" in
  "status server") exit 0 ;;
  "workspace create")
    shift 2
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--cwd" ]; then
        printf '%s' "$2" > "$RK_TEST_HERDR_CWD"
        break
      fi
      shift
    done
    : > "$RK_TEST_HERDR_WORKSPACE"
    printf '%s\n' '{"workspace_id":"ws_king"}'
    ;;
  "api snapshot")
    cwd="$(cat "$RK_TEST_HERDR_CWD" 2>/dev/null || printf /tmp)"
    if [ -f "$RK_TEST_HERDR_AGENT" ]; then
      session="$(cat "$RK_TEST_HERDR_AGENT")"
      # Herdr populates `agent_session` only for harnesses that report one.
      # RK_TEST_HERDR_NO_SESSION reproduces the omitted-session shape a live
      # `claude` agent actually returns, where `revision` is the only fence.
      if [ -n "${RK_TEST_HERDR_NO_SESSION:-}" ]; then
        printf '{"result":{"snapshot":{"panes":[{"workspace_id":"ws_king","pane_id":"pane_king"}],"agents":[{"name":"king","label":"king","terminal_id":"term_king","pane_id":"pane_king","revision":%s,"agent":"claude","cwd":"%s","agent_status":"idle","interactive_ready":true,"focused":false}]}}}\n' "$session" "$cwd"
      else
        printf '{"result":{"snapshot":{"panes":[{"workspace_id":"ws_king","pane_id":"pane_king"}],"agents":[{"name":"king","label":"king","terminal_id":"term_king","pane_id":"pane_king","revision":%s,"agent_session":{"value":"session_%s"},"agent":"codex","cwd":"%s","agent_status":"idle","focused":false}]}}}\n' "$session" "$session" "$cwd"
      fi
    elif [ -f "$RK_TEST_HERDR_WORKSPACE" ]; then
      printf '%s\n' '{"result":{"snapshot":{"panes":[{"workspace_id":"ws_king","pane_id":"pane_king"}],"agents":[]}}}'
    else
      printf '%s\n' '{"result":{"snapshot":{"panes":[],"agents":[]}}}'
    fi
    ;;
  "agent start")
    case "$3" in
      [a-z]*)
        case "$3" in
          *[!a-z0-9_-]*) exit 64 ;;
        esac
        ;;
      *) exit 64 ;;
    esac
    session=1
    if [ -f "$RK_TEST_HERDR_AGENT" ]; then
      session=$(( $(cat "$RK_TEST_HERDR_AGENT") + 1 ))
    fi
    printf '%s' "$session" > "$RK_TEST_HERDR_AGENT"
    ;;
  "agent prompt")
    # Herdr 0.8 resolves agent targets by name or pane id only; a terminal id
    # returns agent_not_found. RK must not address panes by terminal id.
    case "$3" in
      term_*)
        printf '{"error":{"code":"agent_not_found","message":"agent target %s not found"}}\n' "$3" >&2
        exit 1
        ;;
    esac
    exit 0
    ;;
  "agent send")
    # Removed in Herdr 0.8; the real CLI prints usage and fails.
    printf 'herdr agent commands:\n' >&2
    exit 64
    ;;
  "pane close")
    rm -f "$RK_TEST_HERDR_AGENT" "$RK_TEST_HERDR_WORKSPACE"
    ;;
  *)
    printf 'unexpected fake herdr command: %s\n' "$*" >&2
    exit 2
    ;;
esac
"#,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&script, permissions).unwrap();
    (bin, root.join("herdr.log"))
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
    }
    panic!("daemon did not come up");
}

/// Drive the full lifecycle once and return the two generation fences the
/// daemon registered. `report_session` selects which Herdr snapshot shape the
/// fake serves: a harness that reports `agent_session`, or one that does not.
async fn run_lifecycle(report_session: bool) -> (String, String) {
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let (bin, log) = install_fake_herdr(root.path());
    let original_path = std::env::var_os("PATH").unwrap_or_default();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    tokio::time::sleep(Duration::from_millis(100)).await;
    if handle.is_finished() {
        panic!("daemon exited early: {:?}", handle.await);
    }
    let mut client = connect(&layout).await;
    std::env::set_var(
        "PATH",
        std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(&original_path)))
            .unwrap(),
    );
    std::env::set_var("RK_TEST_HERDR_LOG", &log);
    std::env::set_var("RK_TEST_HERDR_CWD", root.path().join("herdr.cwd"));
    std::env::set_var(
        "RK_TEST_HERDR_WORKSPACE",
        root.path().join("herdr.workspace"),
    );
    std::env::set_var("RK_TEST_HERDR_AGENT", root.path().join("herdr.agent"));
    if report_session {
        std::env::remove_var("RK_TEST_HERDR_NO_SESSION");
    } else {
        std::env::set_var("RK_TEST_HERDR_NO_SESSION", "1");
    }

    let spawned = client
        .call("king.spawn", json!({"cwd": root.path(), "holder": "king"}))
        .await
        .unwrap();
    let first = spawned["registration"]["identity"]["session_id"]
        .as_str()
        .expect("spawn registered a generation fence")
        .to_string();

    let restarted = client.call("king.restart", json!({})).await.unwrap();
    assert_eq!(restarted["restarted"], true);
    assert_eq!(restarted["restore_injected"], true);
    let second = restarted["registration"]["identity"]["session_id"]
        .as_str()
        .expect("restart registered a generation fence")
        .to_string();
    assert!(restarted["checkpoint"]
        .as_str()
        .is_some_and(|id| id.starts_with("KCP-")));

    let dismissed = client.call("king.dismiss", json!({})).await.unwrap();
    assert_eq!(dismissed, json!({"dismissed": true, "closed": true}));
    let status = client.call("king.status", json!({})).await.unwrap();
    assert!(status["state"]["registration"].is_null());

    let herdr_log = std::fs::read_to_string(&log).unwrap();
    assert_eq!(herdr_log.matches("agent start king").count(), 2);
    assert!(herdr_log.contains("workspace create"));
    assert!(herdr_log.contains("pane close pane_king"));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
    std::env::set_var("PATH", original_path);
    std::env::remove_var("RK_TEST_HERDR_NO_SESSION");
    (first, second)
}

/// Both cases run in one test: the body mutates the process-wide `PATH`, so
/// two `#[tokio::test]` functions in this binary would race each other.
#[tokio::test]
async fn spawn_restart_and_dismiss_manage_one_registered_king_generation() {
    let (first, second) = run_lifecycle(true).await;
    assert_eq!(first, "session_1");
    assert_eq!(second, "session_2");

    // Claude Code reports no `agent_session`, so Herdr omits the field
    // entirely. The King must still spawn, restart, and fence generations.
    let (first, second) = run_lifecycle(false).await;
    assert_eq!(first, "revision:1");
    assert_eq!(second, "revision:2");
}
