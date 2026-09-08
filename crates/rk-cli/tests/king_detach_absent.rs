//! Regression for the King control-loop log-spam bug: a registration whose
//! Herdr generation has vanished (crash, restart, machine reboot) must warn
//! at most once and then go silent, not fail every single poll forever.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn install_fake_herdr(root: &Path) -> PathBuf {
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let script = bin.join("herdr");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu

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
      printf '{"result":{"snapshot":{"panes":[{"workspace_id":"ws_king","pane_id":"pane_king"}],"agents":[{"name":"king","label":"king","terminal_id":"term_king","pane_id":"pane_king","revision":%s,"agent_session":{"value":"session_%s"},"agent":"codex","cwd":"%s","agent_status":"idle","focused":false}]}}}\n' "$session" "$session" "$cwd"
    elif [ -f "$RK_TEST_HERDR_WORKSPACE" ]; then
      printf '%s\n' '{"result":{"snapshot":{"panes":[{"workspace_id":"ws_king","pane_id":"pane_king"}],"agents":[]}}}'
    else
      printf '%s\n' '{"result":{"snapshot":{"panes":[],"agents":[]}}}'
    fi
    ;;
  "agent start")
    session=1
    if [ -f "$RK_TEST_HERDR_AGENT" ]; then
      session=$(( $(cat "$RK_TEST_HERDR_AGENT") + 1 ))
    fi
    printf '%s' "$session" > "$RK_TEST_HERDR_AGENT"
    ;;
  "agent prompt")
    exit 0
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
    bin
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

/// A registration whose Herdr identity is absent yields at most one warning
/// across N cycles (via `king_cycle` detaching the registration instead of
/// returning an `Err` the periodic loop would otherwise log every poll), and
/// the reappearance of a generation (a fresh `rk king spawn`) resumes normal
/// cycling — see TKT-lupoh-goziv-gokaf / crates/rk-daemon/src/king.rs
/// `detach_absent`.
#[tokio::test]
async fn absent_generation_detaches_once_and_recovers_on_respawn() {
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let bin = install_fake_herdr(root.path());
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
    let agent_path = root.path().join("herdr.agent");
    std::env::set_var("RK_TEST_HERDR_CWD", root.path().join("herdr.cwd"));
    std::env::set_var(
        "RK_TEST_HERDR_WORKSPACE",
        root.path().join("herdr.workspace"),
    );
    std::env::set_var("RK_TEST_HERDR_AGENT", &agent_path);

    let spawned = client
        .call("king.spawn", json!({"cwd": root.path(), "holder": "king"}))
        .await
        .unwrap();
    assert_eq!(spawned["spawned"], true);
    let first_generation = spawned["registration"]["generation"].as_u64().unwrap();

    // A normal cycle with a live generation never touches the detach path.
    let tick = client.call("king.tick", json!({})).await.unwrap();
    assert_eq!(tick["registered"], true);

    // Simulate the Herdr session vanishing under the registered generation:
    // the workspace pane is still there, but the exact agent generation is
    // gone — the crash/restart/reboot case from the bug report.
    std::fs::remove_file(&agent_path).unwrap();

    let detached = client.call("king.tick", json!({})).await.unwrap();
    assert_eq!(detached, json!({"registered": false, "action": "detached"}));

    // Every subsequent cycle with nothing registered is a silent no-op — the
    // same path as never-registered — not a repeated failure.
    for _ in 0..3 {
        let quiet = client.call("king.tick", json!({})).await.unwrap();
        assert_eq!(quiet, json!({"registered": false, "action": "none"}));
    }

    let status = client.call("king.status", json!({})).await.unwrap();
    assert!(status["state"]["registration"].is_null());
    let detachment = &status["state"]["detached"];
    assert_eq!(detachment["holder"], "king");
    assert_eq!(detachment["generation"], first_generation);
    assert!(detachment["reason"].as_str().unwrap().contains("absent"));

    // A later `rk king spawn` (or `rk king register`) clears the detached
    // state and resumes normal cycling under a new generation.
    let respawned = client
        .call("king.spawn", json!({"cwd": root.path(), "holder": "king"}))
        .await
        .unwrap();
    assert_eq!(respawned["spawned"], true);
    assert!(respawned["registration"]["generation"].is_u64());

    let status = client.call("king.status", json!({})).await.unwrap();
    assert!(status["state"]["detached"].is_null());
    assert!(!status["state"]["registration"].is_null());

    let resumed = client.call("king.tick", json!({})).await.unwrap();
    assert_eq!(resumed["registered"], true);

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
    std::env::set_var("PATH", original_path);
}
