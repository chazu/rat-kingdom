//! A supervised harness must stay agent-scoped even if it clears the identity
//! environment and lets `Client` present the operator token from `RK_HOME`.
//!
//! The daemon and harness run as the same Unix user. The root token's 0600 mode
//! therefore does not distinguish them: the authorization boundary must also
//! bind the request to the kernel-observed process that opened the connection.

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

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
    }
    panic!("daemon did not come up");
}

fn elevation_probe(rk_bin: &str) -> String {
    format!(
        r#"
read -r _prompt
echo '{{"type":"system","subtype":"init","session_id":"authority-probe"}}'

"{rk_bin}" done "authority probe complete" >/dev/null 2>&1
echo '{{"type":"result","subtype":"success","is_error":false,"result":"authority probe complete","session_id":"authority-probe","total_cost_usd":0.0,"usage":{{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'

# A harness process can outlive the result it reports (attach-mode processes
# routinely do). Wait for that state transition so Completed cannot erase the
# origin boundary while the process still owns its worktree.
until "{rk_bin}" --json status "$RK_AGENT" 2>/dev/null | grep -q '"state":"completed"'; do :; done

if env -u RK_AGENT -u RK_AUTH_TOKEN "{rk_bin}" monitor --repo authority-probe --once >self-elevation-output 2>&1; then
    echo elevated > self-elevation-result
else
    echo blocked > self-elevation-result
fi
"#
    )
}

#[tokio::test]
async fn clearing_agent_env_does_not_grant_operator_authority() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        elevation_probe(env!("CARGO_BIN_EXE_rk")),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;

    let spawned = operator
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "authority-probe",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let worktree = spawned["agent"]["worktree"].as_str().unwrap();
    let result_path = Path::new(worktree).join("self-elevation-result");

    for _ in 0..100 {
        if result_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let result = std::fs::read_to_string(&result_path)
        .unwrap_or_else(|error| panic!("probe did not publish {}: {error}", result_path.display()));
    let probe_output =
        std::fs::read_to_string(Path::new(worktree).join("self-elevation-output")).unwrap();
    assert_eq!(
        result.trim(),
        "blocked",
        "a supervised harness cleared RK_AGENT/RK_AUTH_TOKEN and gained operator authority: \
         {probe_output}"
    );
}
