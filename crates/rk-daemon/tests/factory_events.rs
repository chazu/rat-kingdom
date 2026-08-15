mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::{path::Path, process::Command, time::Duration};

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
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
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

async fn propose_approve_execute(client: &mut Client) -> Value {
    let proposed = client.call("factory.propose_action", json!({
        "kind":"workflow.run",
        "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}, "coordinator":"coord-a"}
    })).await.unwrap();
    let proposal_id = proposed["proposal"]["id"].as_str().unwrap();
    let digest = proposed["proposal"]["digest"].as_str().unwrap();
    client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap();
    client.call("factory.execute_action", json!({
        "proposal_id": proposal_id,
        "digest": digest,
        "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}, "coordinator":"coord-a"}
    })).await.unwrap()
}

async fn propose_for(client: &mut Client, repo: &str, coordinator: &str, task: &str) -> Value {
    client.call("factory.propose_action", json!({
        "kind":"workflow.run",
        "action":{"name":"factory-test","repo":repo, "params":{"taskId":task}, "coordinator":coordinator}
    })).await.unwrap()
}

#[tokio::test]
async fn test_factory_snapshot_contains_agents_workflows_tickets_inbox_budget_approvals_and_resync()
{
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    client
        .call(
            "space.out",
            json!({
                "category": "need",
                "scope": "repo-a",
                "identity": "human-review",
                "payload": {"text": "review the factory snapshot"},
            }),
        )
        .await
        .unwrap();
    let snapshot = client
        .call("factory.snapshot", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let obj = snapshot["snapshot"].as_object().unwrap();
    let keys: Vec<_> = obj.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        vec![
            "agents",
            "approvals",
            "budget",
            "inbox",
            "repo_resync",
            "tickets",
            "workflows"
        ]
    );
    assert_eq!(obj["inbox"][0]["kind"], "need");
    assert_eq!(obj["inbox"][0]["subject"], "human-review");
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_factory_snapshot_idle_with_long_history_does_not_require_repo_resync() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    for sequence in 0..=rk_daemon::factory_events::MAX_LIMIT {
        propose_for(
            &mut client,
            "repo-a",
            "coord-a",
            &format!("history-{sequence}"),
        )
        .await;
    }

    let snapshot = client
        .call("factory.snapshot", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let replay = client
        .call("factory.events.replay", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    assert_eq!(replay["truncated"], true);
    assert_eq!(snapshot["snapshot"]["repo_resync"]["required"], false);
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_factory_snapshot_filters_repo_and_coordinator_across_sections() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let proposal = propose_for(&mut client, "repo-a", "coord-a", "one").await;
    let proposal_id = proposal["proposal"]["id"].as_str().unwrap().to_string();
    let digest = proposal["proposal"]["digest"].as_str().unwrap().to_string();
    client
        .call(
            "factory.approve_action",
            json!({"proposal_id": proposal_id, "digest": digest}),
        )
        .await
        .unwrap();
    client.call("factory.execute_action", json!({
        "proposal_id": proposal_id,
        "digest": digest,
        "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}, "coordinator":"coord-a"}
    })).await.unwrap();

    let repo_other = client
        .call("factory.snapshot", json!({"repo":"other"}))
        .await
        .unwrap();
    let snap = &repo_other["snapshot"];
    assert!(snap["agents"].as_array().unwrap().is_empty());
    assert!(snap["workflows"].as_array().unwrap().is_empty());
    assert!(snap["tickets"].as_array().unwrap().is_empty());
    assert!(snap["approvals"]["proposals"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(snap["approvals"]["grants"].as_array().unwrap().is_empty());
    assert!(snap["budget"]["repos"].as_array().unwrap().is_empty());
    assert!(snap["budget"]["instances"].as_array().unwrap().is_empty());

    let coord_other = client
        .call(
            "factory.snapshot",
            json!({"repo":"repo-a", "coordinator":"coord-b"}),
        )
        .await
        .unwrap();
    let snap = &coord_other["snapshot"];
    assert!(snap["agents"].as_array().unwrap().is_empty());
    assert!(snap["workflows"].as_array().unwrap().is_empty());
    assert!(snap["approvals"]["proposals"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(snap["approvals"]["grants"].as_array().unwrap().is_empty());
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_approval_lifecycle_emits_approval_changed_events() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    propose_approve_execute(&mut client).await;
    let replay = client
        .call(
            "factory.events.replay",
            json!({"repo":"repo-a", "kinds":["approval.changed"], "limit":20}),
        )
        .await
        .unwrap();
    let states: Vec<_> = replay["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["payload"]["status"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(states, vec!["proposed", "approved", "consumed"]);
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_workflow_run_emits_workflow_event() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let executed = propose_approve_execute(&mut client).await;
    let replay = client
        .call(
            "factory.events.replay",
            json!({"repo":"repo-a", "kinds":["workflow.changed"], "limit":20}),
        )
        .await
        .unwrap();
    assert!(
        replay["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["source"] == "workflow.run"
                && e["subject"]["id"] == executed["instance"]["id"])
    );
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_replay_filters_by_repo_and_kind() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    propose_approve_execute(&mut client).await;
    let replay = client
        .call(
            "factory.events.replay",
            json!({"repo":"repo-a", "kinds":["workflow.changed"], "limit":20}),
        )
        .await
        .unwrap();
    assert!(replay["events"]
        .as_array()
        .unwrap()
        .iter()
        .all(|e| e["repo"] == "repo-a" && e["kind"] == "workflow.changed"));
    let none = client
        .call(
            "factory.events.replay",
            json!({"repo":"other", "kinds":["workflow.changed"], "limit":20}),
        )
        .await
        .unwrap();
    assert!(none["events"].as_array().unwrap().is_empty());
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_replay_boundary_comes_from_unfiltered_scan_when_filtered_page_truncates() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    client.call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}})).await.unwrap();
    client.call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"two"}}})).await.unwrap();
    let replay = client
        .call("factory.events.replay", json!({"repo":"other", "limit":1}))
        .await
        .unwrap();
    assert!(replay["events"].as_array().unwrap().is_empty());
    assert_eq!(replay["truncated"], true);
    assert!(
        replay["boundary"].as_u64().is_some(),
        "sentinel boundary comes from unfiltered durable scan"
    );
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_snapshot_and_replay_are_read_only() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    client.call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"one"}}})).await.unwrap();
    let before = client
        .call("factory.events.replay", json!({"limit":20}))
        .await
        .unwrap();
    client
        .call("factory.snapshot", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    client
        .call(
            "factory.events.replay",
            json!({"repo":"repo-a", "kinds":["approval.changed"], "limit":20}),
        )
        .await
        .unwrap();
    let after = client
        .call("factory.events.replay", json!({"limit":20}))
        .await
        .unwrap();
    assert_eq!(before, after);
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_factory_read_only_methods_permit_authenticated_agent_but_actions_do_not() {
    let (_home, _repo, layout, handle, mut client) = setup().await;
    propose_for(&mut client, "repo-a", "coord-a", "one").await;
    let mut agent = Client::connect_as(&layout, "rat-a").await.unwrap();
    agent
        .call("factory.snapshot", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    agent
        .call(
            "factory.events.replay",
            json!({"repo":"repo-a", "limit":20}),
        )
        .await
        .unwrap();
    let (initial, _stream) = agent
        .call_then_stream("factory.events.watch", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    assert!(initial["events"].is_array());
    let mut agent = Client::connect_as(&layout, "rat-a").await.unwrap();
    let denied = agent
        .call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"blocked"}}}))
        .await
        .unwrap_err();
    assert!(denied.to_string().contains("not authorized"));
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_replay_uses_sentinel_boundary_when_truncated() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    for task in ["one", "two", "three"] {
        client.call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":task}}})).await.unwrap();
    }
    let replay = client
        .call("factory.events.replay", json!({"repo":"repo-a", "limit":1}))
        .await
        .unwrap();
    assert_eq!(replay["events"].as_array().unwrap().len(), 1);
    assert_eq!(replay["truncated"], true);
    assert!(replay["boundary"].as_u64().unwrap() > replay["events"][0]["cursor"].as_u64().unwrap());
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_watch_skips_events_at_or_before_replay_boundary() {
    let (_home, _repo, layout, handle, mut client) = setup().await;
    for task in ["one", "two"] {
        client.call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":task}}})).await.unwrap();
    }
    let replay = client
        .call("factory.events.replay", json!({"repo":"repo-a", "limit":1}))
        .await
        .unwrap();
    let boundary = replay["boundary"].as_u64().unwrap();
    let watcher = connect(&layout).await;
    let (initial, mut stream) = watcher
        .call_then_stream(
            "factory.events.watch",
            json!({"repo":"repo-a", "after": boundary}),
        )
        .await
        .unwrap();
    assert!(initial["events"].as_array().unwrap().is_empty());
    client.call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":"three"}}})).await.unwrap();
    let note = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(note["params"]["cursor"].as_u64().unwrap() > boundary);
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_watch_durable_catchup_after_truncated_boundary() {
    let (_home, _repo, layout, handle, mut client) = setup().await;
    for task in ["one", "two", "three"] {
        propose_for(&mut client, "repo-a", "coord-a", task).await;
    }
    let replay = client
        .call("factory.events.replay", json!({"repo":"repo-a", "limit":1}))
        .await
        .unwrap();
    assert_eq!(replay["truncated"], true);
    let boundary = replay["boundary"].as_u64().unwrap();
    propose_for(&mut client, "repo-a", "coord-a", "durable-after-boundary").await;

    let watcher = connect(&layout).await;
    let (initial, _stream) = watcher
        .call_then_stream(
            "factory.events.watch",
            json!({"repo":"repo-a", "after": boundary, "limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(initial["events"].as_array().unwrap().len(), 1);
    assert!(initial["events"][0]["cursor"].as_u64().unwrap() > boundary);
    assert!(initial["events"][0]["payload"]["proposal_id"].is_string());
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
