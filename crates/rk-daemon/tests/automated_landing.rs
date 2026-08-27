//! Workflow names never grant landing authority. Both managed global and
//! repo-local definitions must pass the configured human gate; an activated
//! per-repo CUE policy then constrains the target and mechanical checks.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

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
    support::install_passing_landing_checks(dir);
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
async fn neither_global_nor_repo_local_steward_bypasses_the_human_gate() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));

    // A managed global definition carries no name-based exception.
    let trusted_home = tempfile::tempdir().unwrap();
    let trusted_repo = tempfile::tempdir().unwrap();
    init_repo(trusted_repo.path());
    let trusted_layout = Layout::at(trusted_home.path());
    std::fs::create_dir_all(trusted_layout.workflows_dir()).unwrap();
    std::fs::write(trusted_layout.workflows_dir().join("steward.cue"), STEWARD).unwrap();
    let trusted_daemon = Daemon::new_in_memory(trusted_layout.clone(), "trusted".into()).unwrap();
    let trusted_handle = tokio::spawn(trusted_daemon.run());
    let mut trusted_client = connect(&trusted_layout).await;
    trusted_client
        .call(
            "repo.add",
            json!({"name": "trusted", "path": trusted_repo.path()}),
        )
        .await
        .unwrap();
    let trusted = run_and_wait(&mut trusted_client, trusted_repo.path()).await;
    assert_eq!(trusted["instance"]["status"], "failed", "{trusted}");
    assert_eq!(
        trusted["instance"]["error"],
        "land step requires a prior approved human gate"
    );
    assert!(
        !trusted_repo.path().join("steward.txt").exists(),
        "managed workflow work must not land without approval"
    );
    trusted_handle.abort();

    // A repository-local definition has the same rule.
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
    local_client
        .call(
            "repo.add",
            json!({"name": "local", "path": local_repo.path()}),
        )
        .await
        .unwrap();
    let local = run_and_wait(&mut local_client, local_repo.path()).await;
    assert_eq!(local["instance"]["status"], "failed", "{local}");
    assert_eq!(
        local["instance"]["error"],
        "land step requires a prior approved human gate"
    );
    assert!(
        !local_repo.path().join("steward.txt").exists(),
        "repo-local name shadowing must not land work"
    );
    local_handle.abort();

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activated_agent_base_policy_does_not_replace_human_approval() {
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
        .call("repo.add", json!({"name": repo_name, "path": repo.path()}))
        .await
        .unwrap();

    let result = run_and_wait(&mut client, repo.path()).await;
    assert_eq!(result["instance"]["status"], "failed", "{result}");
    assert_eq!(result["instance"]["context"]["approval_granted"], false);
    assert!(
        !Command::new("git")
            .arg("-C")
            .arg(repo.path())
            .args(["show", "feature/integration:steward.txt"])
            .output()
            .unwrap()
            .status
            .success(),
        "activated CUE target policy constrains an approved landing; it does not grant approval"
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
