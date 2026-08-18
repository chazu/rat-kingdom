use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

const COMPLETE: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"onboarding-policy"}'
read -r _first_message
echo '{"type":"result","subtype":"success","is_error":false,"result":"assessment complete","session_id":"onboarding-policy","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5}}'
"#;

const POLICY: &str = r#"repo: {
    work: {branch: "work/{{task}}/{{agent}}", worktree: "{{repo}}/{{agent}}"}
    delivery: {target: "agent-base", mode: "merge-push", remote: "origin", remoteBranch: "{{branch}}", deleteSource: true}
}
"#;

fn git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn new_file_diff(target: &str, source: &str) -> String {
    let body = source
        .lines()
        .map(|line| format!("+{line}\n"))
        .collect::<String>();
    format!(
        "diff --git a/{target} b/{target}\nnew file mode 100644\n--- /dev/null\n+++ b/{target}\n@@ -0,0 +1,{} @@\n{body}",
        source.lines().count()
    )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn onboarding_activates_the_exact_repository_policy_into_the_registry() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-b", "main"]);
    git(repo.path(), &["config", "user.email", "test@example.com"]);
    git(repo.path(), &["config", "user.name", "Test"]);
    std::fs::write(repo.path().join("README.md"), "# policy onboarding\n").unwrap();
    git(repo.path(), &["add", "."]);
    git(repo.path(), &["commit", "-m", "initial"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", COMPLETE);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "policy-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let name = repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call("repo.add", json!({"name": name, "path": repo.path()}))
        .await
        .unwrap();
    let started = client
        .call(
            "repo.onboard.start",
            json!({"target": repo.path(), "harness": "fake"}),
        )
        .await
        .unwrap();
    let session = started["session"]["id"].as_str().unwrap();

    let proposed = client
        .call(
            "repo.onboard.propose",
            json!({
                "session": session,
                "proposal": {
                    "kind": "repo_file",
                    "title": "Activate repository delivery policy",
                    "evidence": ["reviewed branch, worktree, target, remote, and delivery behavior"],
                    "target_path": ".rk/repo.cue",
                    "action": "write_repo_file",
                    "diff": new_file_diff(".rk/repo.cue", POLICY),
                    "risk": "high",
                    "verification": ["validate repository policy schema and template containment"],
                },
            }),
        )
        .await
        .unwrap()["proposal"]
        .clone();
    client
        .call(
            "repo.onboard.approve",
            json!({"session": session, "proposal": proposed["id"], "digest": proposed["digest"]}),
        )
        .await
        .unwrap();
    let applied = client
        .call(
            "repo.onboard.apply",
            json!({"session": session, "proposal": proposed["id"], "digest": proposed["digest"]}),
        )
        .await
        .unwrap();
    assert_eq!(applied["proposal"]["status"], "verified");
    assert_eq!(
        applied["proposal"]["validation_results"][0]["automation_kind"],
        "repository_policy"
    );
    assert!(!repo.path().join(".rk/repo.cue").exists());

    let activated = client
        .call(
            "repo.onboard.activate",
            json!({"session": session, "proposal": proposed["id"], "digest": proposed["digest"]}),
        )
        .await
        .unwrap();
    assert_eq!(activated["proposal"]["activation"]["status"], "activated");
    assert!(repo.path().join(".rk/repo.cue").exists());
    let registered: Value = client
        .call("repo.get", json!({"name": name}))
        .await
        .unwrap();
    let approved = &registered["repo"]["activated_policy"];
    assert_eq!(approved["policy"]["delivery"]["mode"], "merge-push");
    assert_eq!(
        approved["digest"],
        activated["proposal"]["activation"]["target_digest"]
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
