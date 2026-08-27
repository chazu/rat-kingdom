//! `rk spawn --ticket` must resolve a repo even when the dispatched ticket is
//! scope "system" but has a parent ticket carrying a concrete repo scope —
//! the shape a sub-ticket minted before parent-scope inheritance landed
//! would have. Without the fallback, `resolve_path` bails with
//! `'system' is neither a path nor a registered repo`, so the sub-ticket can
//! never be dispatched at all.

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

const RESULT_LINE: &str = r#"echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#;

const FAKE_HARNESS: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_ticket_resolves_repo_through_system_scoped_parent_chain() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(repo_dir.path().join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        format!("{FAKE_HARNESS}{RESULT_LINE}\n"),
    );

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": "myrepo", "path": repo_dir.path()}),
        )
        .await
        .unwrap();

    // A repo-scoped parent ticket.
    let parent = client
        .call(
            "ticket.new",
            json!({"title": "parent work", "scope": "myrepo"}),
        )
        .await
        .unwrap();
    let parent_id = parent["ticket"]["identity"].as_str().unwrap().to_string();

    // A sub-ticket that is scope "system" despite its parent being
    // repo-scoped — the shape a pre-fix decomposition would have minted.
    let sub = client
        .call(
            "ticket.new",
            json!({"title": "sub work", "scope": "system", "parent": parent_id}),
        )
        .await
        .unwrap();
    let sub_id = sub["ticket"]["identity"].as_str().unwrap().to_string();
    assert_eq!(sub["ticket"]["scope"], "system");

    let output = Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(["--json", "spawn", "--ticket", &sub_id, "--harness", "fake"])
        .env("RK_HOME", home.path())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "spawn --ticket failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("resolving repo through parent {parent_id}")),
        "expected a note about resolving through the parent, got: {stderr}"
    );

    let agent: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected_root = std::fs::canonicalize(repo_dir.path()).unwrap();
    assert_eq!(
        agent["repo_root"].as_str().unwrap(),
        expected_root.to_string_lossy(),
        "agent must have spawned in the parent-resolved repo, not a bogus 'system' path"
    );
}
