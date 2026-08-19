//! Repository policies are content-bound operator configuration: registration
//! activates the exact `.rk/repo.cue`, spawn uses its naming templates, and
//! dismiss executes its delivery mode and remote-branch template.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "policy work" > policy-work.txt
git add policy-work.txt >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: policy"
echo '{"type":"system","subtype":"init","session_id":"policy-fake"}'
rk_done "policy work complete"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"policy-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const POLICY: &str = r#"
repo: {
    work: {
        branch: "workers/{{role}}/{{task}}/{{agent}}"
        worktree: "custom/{{repo}}/{{task}}/{{agent}}"
    }
    delivery: {
        target: "agent-base"
        mode: "push-branch"
        remote: "origin"
        remoteBranch: "review/{{branch}}"
        deleteSource: true
    }
}
"#;

const MERGE_PUSH_POLICY: &str = r#"
repo: {
    delivery: {
        target: "agent-base"
        mode: "merge-push"
        remote: "origin"
        remoteBranch: "{{branch}}"
        deleteSource: true
    }
}
"#;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&output.stdout).to_string()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activated_policy_controls_names_target_and_remote_delivery() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare", "-b", "main"]);

    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path().join("policyrepo");
    std::fs::create_dir(&repo_path).unwrap();
    let repo = repo_path.as_path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    git(
        repo,
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), POLICY).unwrap();
    std::fs::write(repo.join("README.md"), "# policy test\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    git(repo, &["push", "-u", "origin", "main"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "policy-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let repo_name = repo.file_name().unwrap().to_string_lossy().to_string();

    let added = client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();
    assert!(
        added["repo"]["activated_policy"]["digest"]
            .as_str()
            .is_some_and(|digest| digest.len() == 64),
        "registration must bind the exact policy: {added}"
    );

    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": "Feature 42", "harness": "fake"}),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    assert_eq!(
        branch,
        format!("workers/rat/feature-42/{}", agent.to_ascii_lowercase())
    );
    assert_eq!(spawned["agent"]["target_branch"], "main");
    let expected_worktree = layout
        .worktrees_dir()
        .join("custom")
        .join(&repo_name)
        .join("Feature-42")
        .join(&agent);
    assert_eq!(
        spawned["agent"]["worktree"],
        expected_worktree.to_string_lossy().as_ref()
    );

    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            break;
        }
    }
    let events = client
        .call(
            "space.scan",
            json!({"category": "event", "identity": "harness_result"}),
        )
        .await
        .unwrap();
    let completion = events["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tuple| tuple["payload"]["agent"] == agent)
        .expect("agent completion event");
    assert_eq!(
        completion["payload"]["target"], "main",
        "completion must carry the daemon-authored agent base"
    );
    // C3 (docs/2026-08-17-tkt-c1-generation-identity.md): the producer side
    // of the spawn-keyed join — a real completion must carry the minted
    // generation id, not just the display name.
    assert!(
        completion["payload"]["spawn"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "harness_result must carry the completing generation's spawn id: {completion:?}"
    );
    let delivered = client
        .call("agent.dismiss", json!({"name": agent}))
        .await
        .unwrap();
    assert_eq!(delivered["delivered"], true, "{delivered}");
    assert_eq!(delivered["pushed"], true, "{delivered}");
    assert_eq!(delivered["merged"], false, "{delivered}");
    assert_eq!(delivered["branch_deleted"], true, "{delivered}");
    let remote_branch = format!("review/{branch}");
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(origin.path())
            .args([
                "rev-parse",
                "--verify",
                &format!("refs/heads/{remote_branch}")
            ])
            .output()
            .unwrap()
            .status
            .success(),
        "configured remote branch must exist"
    );
    assert_eq!(
        git(repo, &["rev-parse", "main"]),
        git(origin.path(), &["rev-parse", "main"])
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_push_delivers_the_agents_feature_base_to_the_remote() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare", "-b", "main"]);
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path().join("mergepushrepo");
    std::fs::create_dir(&repo_path).unwrap();
    let repo = repo_path.as_path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    git(
        repo,
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), MERGE_PUSH_POLICY).unwrap();
    std::fs::write(repo.join("README.md"), "# merge push\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    git(repo, &["branch", "feature/integration"]);
    git(repo, &["push", "-u", "origin", "main"]);
    git(repo, &["push", "-u", "origin", "feature/integration"]);
    let main_before = git(repo, &["rev-parse", "main"]);
    let feature_before = git(repo, &["rev-parse", "feature/integration"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "merge-push".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call("repo.add", json!({"name": "friendly-alias", "path": repo}))
        .await
        .unwrap();
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo,
                "task": "feature-policy",
                "harness": "fake",
                "base": "feature/integration",
            }),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    assert_eq!(spawned["agent"]["target_branch"], "feature/integration");
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    let delivered = client
        .call("agent.dismiss", json!({"name": agent}))
        .await
        .unwrap();
    assert_eq!(delivered["delivered"], true, "{delivered}");
    assert_eq!(delivered["merged"], true, "{delivered}");
    assert_eq!(delivered["pushed"], true, "{delivered}");
    assert_eq!(delivered["target"], "feature/integration");
    assert_ne!(
        git(repo, &["rev-parse", "feature/integration"]),
        feature_before
    );
    assert_eq!(git(repo, &["rev-parse", "main"]), main_before);
    assert_eq!(
        git(repo, &["rev-parse", "feature/integration"]),
        git(origin.path(), &["rev-parse", "feature/integration"]),
        "merge-push must advance the configured remote feature branch"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
