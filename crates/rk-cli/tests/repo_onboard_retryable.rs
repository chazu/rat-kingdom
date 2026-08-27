use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

const COMPLETE: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"onboarding-retryable"}'
read -r _first_message
echo '{"type":"result","subtype":"success","is_error":false,"result":"assessment complete","session_id":"onboarding-retryable","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5}}'
"#;

fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn new_file_diff(path: &str, source: &str) -> String {
    let body = source
        .lines()
        .map(|line| format!("+{line}\n"))
        .collect::<String>();
    format!(
        "diff --git a/{path} b/{path}\nnew file mode 100644\n--- /dev/null\n+++ b/{path}\n@@ -0,0 +1,{} @@\n{body}",
        source.lines().count()
    )
}

fn draft(path: &str, source: &str) -> Value {
    json!({
        "kind": "repo_file",
        "title": format!("Write {path}"),
        "evidence": ["deterministic repository inspection"],
        "target_path": path,
        "action": "write_repo_file",
        "diff": new_file_diff(path, source),
        "risk": "low",
        "verification": ["git apply --check and exact target digest"],
    })
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..100 {
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not start");
}

async fn propose(client: &mut Client, session: &str, proposal: Value) -> rk_core::Result<Value> {
    client
        .call(
            "repo.onboard.propose",
            json!({"session": session, "proposal": proposal}),
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn voxel_sequence_preflights_patches_applies_generic_files_in_order_and_refreshes_at_new_head(
) {
    std::env::set_var("RK_FAKE_HARNESS_CMD", COMPLETE);
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-b", "main"]);
    git(repo.path(), &["config", "user.email", "test@example.com"]);
    git(repo.path(), &["config", "user.name", "Test"]);
    std::fs::write(repo.path().join("README.md"), "# fixture\n").unwrap();
    git(repo.path(), &["add", "."]);
    git(repo.path(), &["commit", "-m", "initial"]);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let first = client
        .call(
            "repo.onboard.start",
            json!({"target": repo.path(), "harness": "fake"}),
        )
        .await
        .unwrap();
    let first_id = first["session"]["id"].as_str().unwrap().to_string();
    let first_revision = first["session"]["base_revision"]
        .as_str()
        .unwrap()
        .to_string();
    let worktree = first["session"]["worktree"].as_str().unwrap().to_string();

    let corrupt = json!({
        "kind": "repo_file",
        "title": "Corrupt patch",
        "evidence": ["fixture"],
        "target_path": "BROKEN",
        "action": "write_repo_file",
        "diff": "diff --git a/BROKEN b/BROKEN\nnew file mode 100644\n--- /dev/null\n+++ b/BROKEN\n@@ -0,0 +1 @@\n+broken",
        "risk": "low",
        "verification": ["git apply --check"],
    });
    assert!(propose(&mut client, &first_id, corrupt).await.is_err());
    let status = client
        .call("repo.onboard.status", json!({"session": first_id}))
        .await
        .unwrap();
    assert_eq!(status["session"]["proposals"], json!([]));

    let agents = propose(
        &mut client,
        &first_id,
        draft("AGENTS.md", "# Agent guidance\n"),
    )
    .await
    .unwrap()["proposal"]
        .clone();
    let mise = propose(
        &mut client,
        &first_id,
        draft("mise.toml", "[tasks.verify]\nrun = \"cargo test\"\n"),
    )
    .await
    .unwrap()["proposal"]
        .clone();
    // Both decisions bind the original assessed tree; applications then chain
    // in order without deleting or rebasing either approved proposal.
    client
        .call(
            "repo.onboard.approve",
            json!({"session": first_id, "proposal": agents["id"], "digest": agents["digest"]}),
        )
        .await
        .unwrap();
    client
        .call(
            "repo.onboard.approve",
            json!({"session": first_id, "proposal": mise["id"], "digest": mise["digest"]}),
        )
        .await
        .unwrap();
    let agents_applied = client
        .call(
            "repo.onboard.apply",
            json!({"session": first_id, "proposal": agents["id"], "digest": agents["digest"]}),
        )
        .await
        .unwrap();
    assert_eq!(agents_applied["proposal"]["status"], "verified");
    let mise_applied = client
        .call(
            "repo.onboard.apply",
            json!({"session": first_id, "proposal": mise["id"], "digest": mise["digest"]}),
        )
        .await
        .unwrap();
    assert_eq!(mise_applied["proposal"]["status"], "verified");
    assert!(Path::new(&worktree).join("AGENTS.md").is_file());
    assert!(Path::new(&worktree).join("mise.toml").is_file());
    assert_eq!(
        git(Path::new(&worktree), &["rev-list", "--count", "HEAD"]),
        "3"
    );

    std::fs::write(repo.path().join("NEXT"), "new head\n").unwrap();
    git(repo.path(), &["add", "NEXT"]);
    git(repo.path(), &["commit", "-m", "advance canonical head"]);
    let second = client
        .call(
            "repo.onboard.start",
            json!({"target": repo.path(), "harness": "fake"}),
        )
        .await
        .unwrap();
    assert_eq!(second["reused"], false);
    assert_ne!(second["session"]["id"], first_id);
    assert_ne!(second["session"]["base_revision"], first_revision);

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
