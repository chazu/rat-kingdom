mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::{path::Path, process::Command, time::Duration};
use support::connect;

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

const WORKFLOW: &str = r#"
workflow: {
    name: "factory-test"
    params: { taskId: {type: "string", required: true} }
    agents: { default: {harness: "fake", model: "sonnet"} }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

const WORKING_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
rk_done "work done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

async fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Layout,
    tokio::task::JoinHandle<rk_core::Result<()>>,
    Client,
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    let wf_dir = repo_dir.path().join(".rk/workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("factory-test.cue"), WORKFLOW).unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name":"repo-a", "path": repo_dir.path()}),
        )
        .await
        .unwrap();
    (home, repo_dir, layout, handle, client)
}

#[tokio::test]
async fn test_workflow_run_without_approval_returns_proposal_not_instance() {
    let (_home, repo_dir, _layout, handle, mut client) = setup().await;
    let proposed = client
        .call(
            "factory.propose_action",
            json!({
                "kind":"workflow.run",
                "action":{"name":"factory-test","repo": repo_dir.path(), "params":{"taskId":"one"}}
            }),
        )
        .await
        .unwrap();
    assert!(proposed["proposal"]["digest"].as_str().is_some());
    assert!(proposed.get("instance").is_none());
    let listed = client.call("workflow.list", json!({})).await.unwrap();
    assert!(listed["instances"].as_array().unwrap().is_empty());
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_execute_rejects_tampered_params() {
    let (_home, repo_dir, _layout, handle, mut client) = setup().await;
    let proposed = client
        .call(
            "factory.propose_action",
            json!({
                "kind":"workflow.run",
                "action":{"name":"factory-test","repo": repo_dir.path(), "params":{"taskId":"one"}}
            }),
        )
        .await
        .unwrap();
    let proposal_id = proposed["proposal"]["id"].as_str().unwrap();
    let digest = proposed["proposal"]["digest"].as_str().unwrap();
    client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap();
    let err = client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposal_id,
                "digest": digest,
                "action":{"name":"factory-test","repo": repo_dir.path(), "params":{"taskId":"two"}}
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("forbidden") || err.contains("bad_params"),
        "{err}"
    );
    let listed = client.call("workflow.list", json!({})).await.unwrap();
    assert!(listed["instances"].as_array().unwrap().is_empty());
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_execute_workflow_run_with_matching_approval_starts_instance() {
    let (_home, _repo_dir, _layout, handle, mut client) = setup().await;
    let proposed = client
        .call(
            "factory.propose_action",
            json!({
                "kind":"workflow.run",
                "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}
            }),
        )
        .await
        .unwrap();
    let proposal_id = proposed["proposal"]["id"].as_str().unwrap();
    let digest = proposed["proposal"]["digest"].as_str().unwrap();
    client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap();
    let executed = client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposal_id,
                "digest": digest,
                "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}
            }),
        )
        .await
        .unwrap();
    assert_eq!(executed["instance"]["workflow"], "factory-test");
    assert_eq!(executed["approval"]["status"], "consumed");
    assert_eq!(
        executed["approval"]["instance_id"],
        executed["instance"]["id"]
    );
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_second_approve_action_fails_without_lifecycle_reset_or_second_dispatch() {
    let (_home, _repo_dir, _layout, handle, mut client) = setup().await;
    let proposed = client
        .call(
            "factory.propose_action",
            json!({
                "kind":"workflow.run",
                "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}
            }),
        )
        .await
        .unwrap();
    let proposal_id = proposed["proposal"]["id"].as_str().unwrap().to_string();
    let digest = proposed["proposal"]["digest"].as_str().unwrap().to_string();
    let approved = client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap();
    assert_eq!(approved["approval"]["status"], "approved");

    let err = client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("already approved"), "{err}");

    let executed = client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposal_id,
                "digest": digest,
                "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}
            }),
        )
        .await
        .unwrap();
    assert_eq!(executed["approval"]["status"], "consumed");

    let err = client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("already consumed"), "{err}");

    tokio::time::sleep(Duration::from_millis(200)).await;
    let listed = client.call("workflow.list", json!({})).await.unwrap();
    let instances = listed["instances"].as_array().unwrap();
    assert_eq!(instances.len(), 1, "{listed}");
    assert_eq!(instances[0]["id"], executed["instance"]["id"]);

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_concurrent_execute_approval_dispatches_once() {
    let (_home, _repo_dir, layout, handle, mut client) = setup().await;
    let proposed = client
        .call(
            "factory.propose_action",
            json!({
                "kind":"workflow.run",
                "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}
            }),
        )
        .await
        .unwrap();
    let proposal_id = proposed["proposal"]["id"].as_str().unwrap().to_string();
    let digest = proposed["proposal"]["digest"].as_str().unwrap().to_string();
    client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap();

    let mut c1 = connect(&layout).await;
    let mut c2 = connect(&layout).await;
    let req = json!({
        "proposal_id": proposal_id,
        "digest": digest,
        "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}
    });
    let (first, second) = tokio::join!(
        c1.call("factory.execute_action", req.clone()),
        c2.call("factory.execute_action", req),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first["instance"]["id"], second["instance"]["id"]);
    assert_eq!(
        first["approval"]["instance_id"],
        second["approval"]["instance_id"]
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    let listed = client.call("workflow.list", json!({})).await.unwrap();
    let instances = listed["instances"].as_array().unwrap();
    assert_eq!(instances.len(), 1, "{listed}");
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
