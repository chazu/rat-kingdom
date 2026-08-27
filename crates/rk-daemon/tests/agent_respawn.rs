//! Phase 2 exit criteria, end to end over the socket: spawn a (fake-harness)
//! rat into a real worktree, watch it complete, verify parent routing, dismiss
//! with merge, and confirm main received the work.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::Daemon;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

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

#[allow(dead_code)]
fn git_out(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
}

#[tokio::test]
async fn crashed_agent_is_failed_and_respawnable() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    // A fake that dies immediately without a result event.
    std::env::set_var("RK_FAKE_HARNESS_CMD", "read -r _p; exit 3");
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "doomed-1",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    let mut failed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "failed" {
            failed = true;
            break;
        }
    }
    assert!(failed, "crash was not detected");

    // Respawn reuses the preserved worktree; this fake completes. It must
    // declare `rk done` before its result line — a clean turn that never
    // does now parks the agent as `Paused` (awaiting resume) rather than
    // `Completed`, so the wait below would time out.
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(
            r#"read -r _p; rk_done "recovered"; echo '{"type":"result","subtype":"success","is_error":false,"result":"recovered","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#,
        ),
    );
    client
        .call("agent.respawn", json!({"name": name}))
        .await
        .unwrap();
    let mut recovered = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            assert_eq!(status["agent"]["result"], "recovered");
            recovered = true;
            break;
        }
    }
    assert!(recovered, "respawn did not recover");
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
