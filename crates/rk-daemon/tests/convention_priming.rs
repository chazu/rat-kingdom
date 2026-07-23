//! Stigmergy P6, end to end: a promoted `Convention` in the space is composed
//! into a spawned rat's system prompt as a "Standing conventions" section, so a
//! quorum-promoted norm actually reaches (and binds) the rat — it does not rely
//! on the rat choosing to `rk scan convention`.

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
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Fake harness that captures the system prompt it was primed with into a
/// committed file, so the test can read back exactly what the rat received.
const CAPTURE_PRIME: &str = r#"
read -r _prompt
printf '%s' "$RK_FAKE_SYSTEM_PROMPT" > primed.txt
git add primed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "capture prime"
echo '{"type":"system","subtype":"init","session_id":"fake-prime"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"captured prime","session_id":"fake-prime","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[tokio::test]
async fn promoted_convention_reaches_spawned_rat_prompt() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", CAPTURE_PRIME);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // A system-scope convention (as the reactor's quorum-promotion would emit)
    // and a repo-scope one, to prove both scopes are picked up at spawn.
    client
        .call(
            "space.out",
            json!({
                "category": "convention",
                "scope": "system",
                "identity": "sug-rebase",
                "lifecycle": "furniture",
                "payload": {"text": "Always rebase before merging."},
            }),
        )
        .await
        .unwrap();
    // A repo's scope is its resolved name (the dir basename), not its full path.
    let repo_scope = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call(
            "space.out",
            json!({
                "category": "convention",
                "scope": repo_scope,
                "identity": "sug-tabs",
                "lifecycle": "furniture",
                "payload": {"text": "This repo uses tabs, not spaces."},
            }),
        )
        .await
        .unwrap();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "prime-1",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "agent never completed");

    // Merge the captured prompt onto main and read it back.
    let dismissed = client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(dismissed["merged"], true, "detail: {}", dismissed["detail"]);

    let primed = git_out(repo_dir.path(), &["show", "main:primed.txt"]);
    assert!(
        primed.contains("## Standing conventions"),
        "prompt should carry a Standing conventions section:\n{primed}"
    );
    assert!(
        primed.contains("- Always rebase before merging."),
        "system-scope convention missing from prompt:\n{primed}"
    );
    assert!(
        primed.contains("- This repo uses tabs, not spaces."),
        "repo-scope convention missing from prompt:\n{primed}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
