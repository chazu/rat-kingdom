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

if [ -n "${RK_TEST_HERDR_DOWN:-}" ]; then
  printf '%s\n' '{"error":{"code":"server_not_running","message":"no herdr server is running"}}' >&2
  exit 1
fi

pane="${RK_TEST_HERDR_MOVED_PANE:-pane_king}"
alias=""
if [ -n "${RK_TEST_HERDR_ALIAS:-}" ]; then
  alias='{"name":"term_king","terminal_id":"term_impostor","pane_id":"pane_impostor","revision":1070,"agent":"codex","cwd":"/tmp","agent_session":{"value":"session_1"}},'
fi

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
      revision="$(cat "$RK_TEST_HERDR_REVISION" 2>/dev/null || printf '%s' "$session")"
      agent=codex
      if [ -n "${RK_TEST_HERDR_NO_SESSION:-}" ]; then agent=claude; fi
      # Herdr populates `agent_session` only for harnesses that report one.
      # RK_TEST_HERDR_NO_SESSION reproduces the omitted-session shape a live
      # `claude` agent actually returns, where `revision` is the only fence.
      delayed=0
      if [ -n "${RK_TEST_HERDR_DELAY_SESSION:-}" ]; then
        delayed="$(cat "$RK_TEST_HERDR_DELAY_SESSION" 2>/dev/null || printf 0)"
      fi
      if [ -n "${RK_TEST_HERDR_NO_SESSION:-}" ] || { [ -n "${RK_TEST_HERDR_DELAY_SESSION:-}" ] && [ "$delayed" -lt 2 ]; }; then
        if [ -n "${RK_TEST_HERDR_DELAY_SESSION:-}" ]; then
          printf '%s' "$((delayed + 1))" > "$RK_TEST_HERDR_DELAY_SESSION"
        fi
        printf '{"result":{"snapshot":{"panes":[{"workspace_id":"ws_king","pane_id":"%s"}],"agents":[%s{"name":"king","label":"king","terminal_id":"term_king","pane_id":"%s","revision":%s,"agent":"%s","cwd":"%s","agent_status":"idle","interactive_ready":true,"focused":%s}]}}}\n' "$pane" "$alias" "$pane" "$revision" "$agent" "$cwd" "${RK_TEST_HERDR_FOCUSED:-false}"
      else
        printf '{"result":{"snapshot":{"panes":[{"workspace_id":"ws_king","pane_id":"%s"}],"agents":[%s{"name":"king","label":"king","terminal_id":"term_king","pane_id":"%s","revision":%s,"agent_session":{"value":"session_%s"},"agent":"codex","cwd":"%s","agent_status":"idle","focused":%s}]}}}\n' "$pane" "$alias" "$pane" "$revision" "$session" "$cwd" "${RK_TEST_HERDR_FOCUSED:-false}"
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
      pane_impostor|term_*)
        printf '{"error":{"code":"agent_not_found","message":"agent target %s not found"}}\n' "$3" >&2
        exit 1
        ;;
    esac
    if [ -n "${RK_TEST_HERDR_MOVED_PANE:-}" ] && [ "$3" = "pane_king" ]; then exit 1; fi
    exit 0
    ;;
  "agent attach")
    if [ "$3" != "$pane" ]; then exit 1; fi
    ;;
  "pane close")
    if [ "$3" != "$pane" ]; then exit 1; fi
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
/// daemon registered. `delay_session` reproduces Codex's live startup race:
/// the first ready snapshot has only `revision`, then `agent_session` appears.
async fn run_lifecycle(report_session: bool, delay_session: bool) -> (String, String) {
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
    let revision_path = root.path().join("herdr.revision");
    std::env::set_var("RK_TEST_HERDR_REVISION", &revision_path);
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
    if delay_session {
        std::env::set_var(
            "RK_TEST_HERDR_DELAY_SESSION",
            root.path().join("herdr.delay-session"),
        );
    } else {
        std::env::remove_var("RK_TEST_HERDR_DELAY_SESSION");
    }

    std::env::set_var("RK_TEST_HERDR_DOWN", "1");
    let error = client
        .call("king.spawn", json!({"cwd": root.path(), "holder": "king"}))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Herdr is not running; start it with `herdr`"),
        "unexpected preflight error: {error}"
    );
    std::env::remove_var("RK_TEST_HERDR_DOWN");

    let spawned = client
        .call("king.spawn", json!({"cwd": root.path(), "holder": "king"}))
        .await
        .unwrap();
    let first = spawned["registration"]["identity"]["session_id"]
        .as_str()
        .expect("spawn registered a generation fence")
        .to_string();

    if report_session && !delay_session {
        // Real Herdr snapshots change metadata revision while the exact
        // terminal and reported harness session remain the same.
        std::fs::write(&revision_path, "1070").unwrap();
        std::env::set_var("RK_TEST_HERDR_ALIAS", "1");
    }
    std::env::set_var("RK_TEST_HERDR_FOCUSED", "true");
    client
        .call(
            "space.out",
            json!({"category": "obstacle", "scope": "system",
        "identity": "fleet", "payload": {"type": "budget_exceeded", "cost_usd": 12}}),
        )
        .await
        .unwrap();
    let queued = client.call("king.tick", json!({})).await.unwrap();
    assert_eq!(
        queued["action"], "wake_pending",
        "focused conversation must retain its turn"
    );
    assert!(!std::fs::read_to_string(&log)
        .unwrap()
        .contains("RK_WAKE KWK-"));
    std::env::set_var("RK_TEST_HERDR_FOCUSED", "false");
    let delivered = client.call("king.tick", json!({})).await.unwrap();
    assert_eq!(delivered["action"], "wake");
    let wake = delivered["wake"].as_str().unwrap();
    let pulled = client
        .call("king.pull", json!({"wake": wake, "holder": "king"}))
        .await
        .unwrap();
    assert_eq!(pulled["snapshot"]["decisions"].as_array().unwrap().len(), 1);
    client
        .call(
            "king.settle",
            json!({"wake": wake, "holder": "king", "disposition": "deferred"}),
        )
        .await
        .unwrap();
    assert_eq!(
        client.call("king.tick", json!({})).await.unwrap()["action"],
        "none"
    );
    assert_eq!(
        std::fs::read_to_string(&log)
            .unwrap()
            .matches("RK_WAKE KWK-")
            .count(),
        1
    );
    std::env::remove_var("RK_TEST_HERDR_FOCUSED");

    if report_session && !delay_session {
        std::env::set_var("RK_TEST_HERDR_MOVED_PANE", "pane_moved");
    }
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

    if report_session && !delay_session {
        let attached = tokio::process::Command::new(env!("CARGO_BIN_EXE_rk"))
            .args(["king", "at"])
            .env("RK_HOME", home.path())
            .output()
            .await
            .unwrap();
        assert!(
            attached.status.success(),
            "{}",
            String::from_utf8_lossy(&attached.stderr)
        );
        assert!(std::fs::read_to_string(&log)
            .unwrap()
            .contains("agent attach pane_moved"));

        // An externally replaced session in that same live terminal must be
        // detached before any prompt can reach the unregistered successor.
        std::fs::write(root.path().join("herdr.agent"), "3").unwrap();
        let prompts_before = std::fs::read_to_string(&log)
            .unwrap()
            .matches("agent prompt")
            .count();
        let tick = client.call("king.tick", json!({})).await.unwrap();
        assert_eq!(tick["action"], "detached");
        let status = client.call("king.status", json!({})).await.unwrap();
        assert!(status["state"]["registration"].is_null());
        assert_eq!(
            std::fs::read_to_string(&log)
                .unwrap()
                .matches("agent prompt")
                .count(),
            prompts_before
        );
        let registered = client
            .call(
                "king.register",
                json!({"target": "term_king", "holder": "king", "name": "king"}),
            )
            .await
            .unwrap();
        assert_eq!(
            registered["registration"]["identity"]["session_id"],
            "agent-session:session_3"
        );
    }

    let dismissed = client.call("king.dismiss", json!({})).await.unwrap();
    assert_eq!(dismissed, json!({"dismissed": true, "closed": true}));
    let status = client.call("king.status", json!({})).await.unwrap();
    assert!(status["state"]["registration"].is_null());

    let herdr_log = std::fs::read_to_string(&log).unwrap();
    assert_eq!(herdr_log.matches("agent start king").count(), 2);
    assert!(herdr_log.contains("workspace create"));
    if report_session && !delay_session {
        assert!(herdr_log.contains("agent prompt pane_moved /exit"));
        assert!(herdr_log.contains("--pane pane_moved"));
        assert!(herdr_log.contains("pane close pane_moved"));
        assert!(!herdr_log.contains("agent prompt pane_impostor"));
    } else {
        assert!(herdr_log.contains("pane close pane_king"));
    }

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
    std::env::set_var("PATH", original_path);
    std::env::remove_var("RK_TEST_HERDR_NO_SESSION");
    std::env::remove_var("RK_TEST_HERDR_DELAY_SESSION");
    std::env::remove_var("RK_TEST_HERDR_DOWN");
    std::env::remove_var("RK_TEST_HERDR_REVISION");
    std::env::remove_var("RK_TEST_HERDR_ALIAS");
    std::env::remove_var("RK_TEST_HERDR_MOVED_PANE");
    (first, second)
}

/// All cases run in one test: the body mutates the process-wide `PATH`, so
/// two `#[tokio::test]` functions in this binary would race each other.
#[tokio::test]
async fn spawn_restart_and_dismiss_manage_one_registered_king_generation() {
    let (first, second) = run_lifecycle(true, true).await;
    assert_eq!(first, "revision:1");
    assert_eq!(second, "agent-session:session_2");

    // Claude Code reports no `agent_session`, so Herdr omits the field
    // entirely. The King must still spawn, restart, and fence generations.
    let (first, second) = run_lifecycle(false, false).await;
    assert_eq!(first, "revision:1");
    assert_eq!(second, "revision:2");

    let (first, second) = run_lifecycle(true, false).await;
    assert_eq!(first, "agent-session:session_1");
    assert_eq!(second, "agent-session:session_2");
}
