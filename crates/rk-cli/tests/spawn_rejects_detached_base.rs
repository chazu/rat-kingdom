//! `rk spawn --base <ref>` names a landing target branch, not a starting
//! commit. An operator (or a workflow) that accidentally passes a raw commit
//! SHA gets it silently persisted as an unmergeable `target_branch`, and the
//! landing queue later hot-loops forever on "merge target does not exist"
//! without ever running a gate (TKT-kujab-momum-vazug). This must be refused
//! at the real public boundary — the `rk` binary talking to a live daemon —
//! before any worktree/provider generation is created and before a
//! dispatched ticket flips to `in_progress`, for every role alike (not just
//! the ordinary `rat` role a mistaken dispatch usually hits). A valid branch
//! must keep working through the same path.

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

fn rev_parse(dir: &Path, rev: &str) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", rev])
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8_lossy(&out.stdout).trim().to_string()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_refuses_a_detached_commit_base_before_any_side_effect() {
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
    // A real commit that is deliberately not a branch tip — exactly the
    // shape of the operator mistake this guards against.
    let detached_sha = rev_parse(repo_dir.path(), "main");
    std::fs::write(repo_dir.path().join("README.md"), "# x\n\nmore\n").unwrap();
    git(repo_dir.path(), &["commit", "-am", "advance main"]);

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

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "ordinary work", "scope": "myrepo"}),
        )
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_rk"))
        .args([
            "--json",
            "spawn",
            "--ticket",
            &ticket_id,
            "--harness",
            "fake",
            "--base",
            &detached_sha,
        ])
        .env("RK_HOME", home.path())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "a detached-commit --base must be refused, got: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not an existing branch"),
        "expected a clear refusal reason, got: {stderr}"
    );

    // No side effect: the ticket must not have flipped to in_progress, and
    // no agent/worktree/generation exists for it.
    let after = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(
        after["ticket"]["payload"]["status"], "open",
        "a refused dispatch must leave the ticket untouched, not phantom in_progress: {after}"
    );
    let agents = client.call("agent.list", json!({})).await.unwrap();
    assert!(
        agents["agents"].as_array().unwrap().is_empty(),
        "no agent/worktree/generation may exist for a refused spawn: {agents}"
    );

    // The same refusal applies to a reviewer role, not only the ordinary
    // `rat` role a mistaken dispatch usually hits.
    let reviewer_output = Command::new(env!("CARGO_BIN_EXE_rk"))
        .args([
            "--json",
            "spawn",
            "--task",
            "ad-hoc-review",
            "--role",
            "reviewer",
            "--harness",
            "fake",
            "--repo",
            "myrepo",
            "--base",
            &detached_sha,
        ])
        .env("RK_HOME", home.path())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(
        !reviewer_output.status.success(),
        "a reviewer dispatch must not be exempt from the same base-branch check, got: stdout={} stderr={}",
        String::from_utf8_lossy(&reviewer_output.stdout),
        String::from_utf8_lossy(&reviewer_output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&reviewer_output.stderr).contains("not an existing branch"),
        "expected the same refusal reason for a reviewer dispatch"
    );

    // A real branch keeps working through the exact same path.
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#,
    );
    let valid = Command::new(env!("CARGO_BIN_EXE_rk"))
        .args([
            "--json",
            "spawn",
            "--ticket",
            &ticket_id,
            "--harness",
            "fake",
            "--base",
            "main",
        ])
        .env("RK_HOME", home.path())
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(
        valid.status.success(),
        "ordinary branch-based dispatch must keep working: stdout={} stderr={}",
        String::from_utf8_lossy(&valid.stdout),
        String::from_utf8_lossy(&valid.stderr)
    );
    let after_valid = client
        .call("ticket.get", json!({"id": ticket_id}))
        .await
        .unwrap();
    assert_eq!(after_valid["ticket"]["payload"]["status"], "in_progress");
}
