//! Automated landing is an operator-granted capability for managed global
//! workflows, not a name-based escape hatch available to repo-local files.

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
echo "trusted workflow landed" > steward.txt
git add steward.txt >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"auto-land-fake"}'
rk_done "work done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"auto-land-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const STEWARD: &str = r#"
workflow: {
    name: "steward"
    agents: {default: {harness: "fake"}}
    steps: [
        {type: "spawn", role: "reviewer", task: {title: "reviewed-work"}},
        {type: "wait", timeout: "60s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss", noMerge: true},
        {type: "land", branch: "{{ctx.activeBranch}}", target: "main"},
        {type: "evaluate", expect: {merged: true}},
    ]
}
"#;

const FEATURE_STEWARD: &str = r#"
workflow: {
    name: "steward"
    agents: {default: {harness: "fake"}}
    steps: [
        {type: "spawn", role: "reviewer", task: {title: "reviewed-feature-work"}},
        {type: "wait", timeout: "60s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss", noMerge: true},
        {type: "land", branch: "{{ctx.activeBranch}}", target: "feature/integration"},
        {type: "evaluate", expect: {delivered: true}},
    ]
}
"#;

const AGENT_BASE_POLICY: &str = r#"
repo: {
    delivery: {target: "agent-base", mode: "merge", remote: "origin", remoteBranch: "{{branch}}", deleteSource: true}
}
"#;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
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

async fn run_and_wait(client: &mut Client, repo: &Path) -> serde_json::Value {
    let started = client
        .call(
            "workflow.run",
            json!({"name": "steward", "repo": repo.to_string_lossy(), "params": {}}),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap();
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        if status["instance"]["status"] != "running" {
            return status;
        }
    }
    panic!("workflow did not settle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_managed_global_steward_may_land_without_a_human_gate() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));

    // The managed global definition is explicitly trusted by policy.
    let trusted_home = tempfile::tempdir().unwrap();
    let trusted_repo = tempfile::tempdir().unwrap();
    init_repo(trusted_repo.path());
    let trusted_layout = Layout::at(trusted_home.path());
    std::fs::create_dir_all(trusted_layout.workflows_dir()).unwrap();
    std::fs::write(trusted_layout.workflows_dir().join("steward.cue"), STEWARD).unwrap();
    let trusted_daemon = Daemon::new_in_memory(trusted_layout.clone(), "trusted".into()).unwrap();
    let trusted_handle = tokio::spawn(trusted_daemon.run());
    let mut trusted_client = connect(&trusted_layout).await;
    let trusted = run_and_wait(&mut trusted_client, trusted_repo.path()).await;
    assert_eq!(trusted["instance"]["status"], "completed", "{trusted}");
    assert!(
        trusted_repo.path().join("steward.txt").exists(),
        "managed steward work must land on main"
    );
    trusted_handle.abort();

    // A repository cannot shadow the trusted name and inherit that authority.
    let local_home = tempfile::tempdir().unwrap();
    let local_repo = tempfile::tempdir().unwrap();
    init_repo(local_repo.path());
    let local_dir = local_repo.path().join(".rk/workflows");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::write(local_dir.join("steward.cue"), STEWARD).unwrap();
    let local_layout = Layout::at(local_home.path());
    let local_daemon = Daemon::new_in_memory(local_layout.clone(), "local".into()).unwrap();
    let local_handle = tokio::spawn(local_daemon.run());
    let mut local_client = connect(&local_layout).await;
    let local = run_and_wait(&mut local_client, local_repo.path()).await;
    assert_eq!(local["instance"]["status"], "failed", "{local}");
    assert_eq!(
        local["instance"]["error"],
        "land step requires a prior approved human gate or a trusted automated workflow"
    );
    assert!(
        !local_repo.path().join("steward.txt").exists(),
        "repo-local name shadowing must not land work"
    );
    local_handle.abort();

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activated_agent_base_policy_authorizes_unattended_feature_branch_landing() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::create_dir_all(repo.path().join(".rk")).unwrap();
    std::fs::write(repo.path().join(".rk/repo.cue"), AGENT_BASE_POLICY).unwrap();
    git(repo.path(), &["add", ".rk/repo.cue"]);
    git(repo.path(), &["commit", "-m", "policy"]);
    git(repo.path(), &["branch", "feature/integration"]);

    let layout = Layout::at(home.path());
    std::fs::create_dir_all(layout.workflows_dir()).unwrap();
    std::fs::write(layout.workflows_dir().join("steward.cue"), FEATURE_STEWARD).unwrap();
    let daemon = Daemon::new_in_memory(layout.clone(), "feature-policy".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let repo_name = repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo.path()}),
        )
        .await
        .unwrap();

    let result = run_and_wait(&mut client, repo.path()).await;
    assert_eq!(result["instance"]["status"], "completed", "{result}");
    assert_eq!(result["instance"]["context"]["approval_granted"], false);
    assert_eq!(
        result["instance"]["context"]["previous_result"]["target"],
        "feature/integration"
    );
    assert_eq!(
        result["instance"]["context"]["previous_result"]["delivered"],
        true
    );
    assert!(
        git(repo.path(), &["show", "feature/integration:steward.txt"])
            .contains("trusted workflow landed")
    );
    assert!(
        !Command::new("git")
            .arg("-C")
            .arg(repo.path())
            .args(["show", "main:steward.txt"])
            .output()
            .unwrap()
            .status
            .success(),
        "main must remain untouched"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
