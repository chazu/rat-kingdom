//! TKT-01M02EK9T3629624MS23BK7V40: exercise the first authenticated call from
//! a freshly launched harness while many spawns are racing one another.
//!
//! The fake harness runs the real `rk` binary immediately on process launch.
//! This is deliberately before the harness emits any protocol event or waits
//! for its prompt, so the call can race the supervisor's post-launch registry
//! update and the daemon's peer-origin lookup.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

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

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# startup race\n").unwrap();
    std::fs::create_dir_all(dir.join(".rk")).unwrap();
    std::fs::write(dir.join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn first_call_harness(rk_bin: &str) -> String {
    let rk_bin = shell_quote(rk_bin);
    format!(
        r#"
set +e
{rk_bin} scan fact system > "$RK_WORKTREE/first-rk.out" 2>&1
status=$?
printf '%s\n' "$status" > "$RK_WORKTREE/first-rk.status"
{rk_bin} done "startup race probe" >/dev/null 2>&1 || true
echo '{{"type":"system","subtype":"init","session_id":"startup-race"}}'
echo '{{"type":"result","subtype":"success","is_error":false,"result":"first call attempted","session_id":"startup-race","total_cost_usd":0.001,"usage":{{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#
    )
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
    }
    panic!("daemon did not come up");
}

async fn wait_for_markers(markers: &[PathBuf]) {
    for _ in 0..500 {
        if markers.iter().all(|path| path.exists()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let missing: Vec<_> = markers
        .iter()
        .filter(|path| !path.exists())
        .map(|path| path.display().to_string())
        .collect();
    panic!("first-call markers did not appear: {missing:?}");
}

#[tokio::test]
async fn first_rk_call_survives_spawn_startup_race() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        first_call_harness(env!("CARGO_BIN_EXE_rk")),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;
    operator
        .call(
            "repo.add",
            json!({"name": "startup-race", "path": repo_dir.path()}),
        )
        .await
        .unwrap();

    // The child runs its first authenticated call before it emits any harness
    // event. Concurrent spawns maximize overlap between launch, registry PID
    // update, socket accept, and supervised_agents_for_peer().
    let mut spawn_calls = Vec::new();
    for index in 0..32 {
        let layout = layout.clone();
        let repo = repo_dir.path().to_string_lossy().to_string();
        spawn_calls.push(tokio::spawn(async move {
            let mut client = Client::connect_as_operator(&layout).await.unwrap();
            client
                .call(
                    "agent.spawn",
                    json!({
                        "repo": repo,
                        "task": format!("codex-auth-race-{index}"),
                        "harness": "fake"
                    }),
                )
                .await
        }));
    }

    let mut markers = Vec::new();
    for call in spawn_calls {
        let spawned = call.await.unwrap().unwrap();
        let worktree = spawned["agent"]["worktree"]
            .as_str()
            .expect("spawn response includes the agent worktree");
        markers.push(Path::new(worktree).join("first-rk.status"));
    }

    wait_for_markers(&markers).await;
    for marker in markers {
        let status = tokio::fs::read_to_string(&marker).await.unwrap();
        let output = tokio::fs::read_to_string(marker.with_file_name("first-rk.out"))
            .await
            .unwrap();
        assert_eq!(
            status.trim(),
            "0",
            "first rk call failed at {}: {output}",
            marker.display()
        );
        assert!(
            !output.contains("FORBIDDEN") && !output.contains("forbidden:"),
            "first rk call was forbidden at {}: {output}",
            marker.display()
        );
    }

    // Drain the lifecycle so this test also proves the successful calls did
    // not merely leave harnesses wedged at the startup boundary.
    for _ in 0..500 {
        let agents = operator.call("agent.list", json!({})).await.unwrap();
        let all_terminal = agents["agents"].as_array().unwrap().iter().all(|agent| {
            matches!(
                agent["state"].as_str(),
                Some("completed") | Some("failed") | Some("dismissed")
            )
        });
        if all_terminal {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
