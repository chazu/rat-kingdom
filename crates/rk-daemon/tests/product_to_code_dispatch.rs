//! Daemon integration tests for the typed `product_to_code.dispatch` action.
//!
//! Dispatch reuses the Phase 2 canonical proposal machinery: propose ->
//! authenticated operator approval of the exact digest -> execute with
//! status/digest/CAS checks. The executor dispatches `implement-featureset`
//! workflow runs for unblocked minted tickets only.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::{path::Path, process::Command, time::Duration};

fn fixture_json(name: &str) -> Value {
    let path = format!(
        "{}/tests/fixtures/product_to_code/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn git(root: &Path, args: &[&str]) {
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
}

const IMPLEMENT_FEATURESET: &str = r#"
workflow: {
    name: "implement-featureset"
    params: {
        taskId: {type: "string", required: true}
        taskDescription: {type: "string", required: true}
    }
    agents: {default: {harness: "fake"}}
    steps: [
        {type: "gate", gateType: "timer", duration: "1s"},
    ]
}
"#;

fn repository() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("rk-p2c-dispatch")
        .tempdir()
        .unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "test@example.com"]);
    git(dir.path(), &["config", "user.name", "Test"]);
    std::fs::write(dir.path().join("README.md"), "# Fixture\n").unwrap();
    let wf_dir = dir.path().join(".rk/workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("implement-featureset.cue"),
        IMPLEMENT_FEATURESET,
    )
    .unwrap();
    git(dir.path(), &["add", "."]);
    git(dir.path(), &["commit", "-m", "initial"]);
    dir
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

async fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Layout,
    tokio::task::JoinHandle<rk_core::Result<()>>,
    Client,
) {
    let home = tempfile::tempdir().unwrap();
    let repo = repository();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": "fixture", "path": repo.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    (home, repo, layout, handle, client)
}

/// Drive the canonical Phase 2 ticket_graph.apply path to completion so a
/// graph-node-id -> minted TKT-id mapping exists for dispatch proposals.
async fn apply_graph(client: &mut Client) -> (String, Value) {
    let action = json!({
        "repo": "fixture",
        "graph": fixture_json("ticket_graph_valid.json"),
        "initiative": fixture_json("initiative_minimal.json"),
    });
    let proposed = client
        .call(
            "factory.propose_action",
            json!({"kind": "ticket_graph.apply", "action": action}),
        )
        .await
        .unwrap();
    client
        .call(
            "factory.approve_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": proposed["proposal"]["digest"],
            }),
        )
        .await
        .unwrap();
    let executed = client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": proposed["proposal"]["digest"],
                "kind": "ticket_graph.apply",
                "action": action,
            }),
        )
        .await
        .unwrap();
    (
        proposed["proposal"]["id"].as_str().unwrap().to_string(),
        executed["result"]["graph_node_to_ticket_id"].clone(),
    )
}

fn dispatch_action(graph_apply_proposal_id: &str) -> Value {
    json!({
        "repo": "fixture",
        "initiative": fixture_json("initiative_minimal.json"),
        "graph": fixture_json("ticket_graph_valid.json"),
        "graph_id": "GRAPH-product-to-code",
        "graph_apply_proposal_id": graph_apply_proposal_id,
        "dispatches": [
            {"graph_node_id": "NODE-contracts", "task_description": "Create serde and CUE contracts"}
        ],
        "blocked": [
            {"graph_node_id": "NODE-tests", "reasons": ["dispatch gate requires current impact evidence covering ticket NODE-tests or its feature set"]}
        ],
    })
}

#[tokio::test]
async fn test_dispatch_rejects_graph_apply_from_other_repo_or_graph_revision() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let (graph_proposal, _mapping) = apply_graph(&mut client).await;

    let other_repo = repository();
    client
        .call(
            "repo.add",
            json!({"name": "other", "path": other_repo.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    let mut other_repo_action = dispatch_action(&graph_proposal);
    other_repo_action["repo"] = json!("other");
    let err = client
        .call(
            "factory.propose_action",
            json!({"kind": "product_to_code.dispatch", "action": other_repo_action}),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("repository"), "{err}");

    let mut changed_graph_action = dispatch_action(&graph_proposal);
    changed_graph_action["graph"]["nodes"][0]["description"] =
        json!("Changed after the graph apply");
    let err = client
        .call(
            "factory.propose_action",
            json!({"kind": "product_to_code.dispatch", "action": changed_graph_action}),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("graph revision"), "{err}");

    let mut changed_initiative_action = dispatch_action(&graph_proposal);
    changed_initiative_action["initiative"]["title"] = json!("Changed after the graph apply");
    let err = client
        .call(
            "factory.propose_action",
            json!({"kind": "product_to_code.dispatch", "action": changed_initiative_action}),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("initiative revision"), "{err}");

    stop(client, handle).await;
}

async fn propose_dispatch(client: &mut Client, graph_apply_proposal_id: &str) -> Value {
    client
        .call(
            "factory.propose_action",
            json!({
                "kind": "product_to_code.dispatch",
                "action": dispatch_action(graph_apply_proposal_id),
            }),
        )
        .await
        .unwrap()
}

async fn approve(client: &mut Client, proposed: &Value) {
    client
        .call(
            "factory.approve_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": proposed["proposal"]["digest"],
            }),
        )
        .await
        .unwrap();
}

async fn execute_dispatch(
    client: &mut Client,
    proposed: &Value,
    graph_apply_proposal_id: &str,
) -> rk_core::Result<Value> {
    client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": proposed["proposal"]["digest"],
                "kind": "product_to_code.dispatch",
                "action": dispatch_action(graph_apply_proposal_id),
            }),
        )
        .await
}

async fn workflow_instances(client: &mut Client) -> Vec<Value> {
    client.call("workflow.list", json!({})).await.unwrap()["instances"]
        .as_array()
        .unwrap()
        .clone()
}

async fn stop(mut client: Client, handle: tokio::task::JoinHandle<rk_core::Result<()>>) {
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_daemon_workflow_dispatch_requires_exact_phase2_operator_approval() {
    // Wrong status: proposed but never approved.
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let (graph_proposal, _mapping) = apply_graph(&mut client).await;
    let proposed = propose_dispatch(&mut client, &graph_proposal).await;
    let err = execute_dispatch(&mut client, &proposed, &graph_proposal)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("not approved") || err.contains("forbidden"),
        "{err}"
    );
    assert!(workflow_instances(&mut client).await.is_empty());
    stop(client, handle).await;

    // Wrong digest: approved, but executed with a tampered digest.
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let (graph_proposal, _mapping) = apply_graph(&mut client).await;
    let proposed = propose_dispatch(&mut client, &graph_proposal).await;
    approve(&mut client, &proposed).await;
    let err = client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": "0".repeat(64),
                "kind": "product_to_code.dispatch",
                "action": dispatch_action(&graph_proposal),
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("digest mismatch") || err.contains("forbidden"),
        "{err}"
    );
    assert!(workflow_instances(&mut client).await.is_empty());
    stop(client, handle).await;

    // Not an authenticated operator: approval and execution are operator-gated.
    let (_home, _repo, layout, handle, mut client) = setup().await;
    let (graph_proposal, _mapping) = apply_graph(&mut client).await;
    let proposed = propose_dispatch(&mut client, &graph_proposal).await;
    approve(&mut client, &proposed).await;
    let mut rat = Client::connect_as(&layout, "rat-a").await.unwrap();
    let err = execute_dispatch(&mut rat, &proposed, &graph_proposal)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("forbidden") || err.contains("unauthorized"),
        "{err}"
    );
    assert!(workflow_instances(&mut client).await.is_empty());
    stop(client, handle).await;

    // CAS mismatch: the ticket store moved between propose and execute.
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let (graph_proposal, _mapping) = apply_graph(&mut client).await;
    let proposed = propose_dispatch(&mut client, &graph_proposal).await;
    approve(&mut client, &proposed).await;
    client
        .call(
            "ticket.new",
            json!({"title": "Concurrent work", "scope": "fixture"}),
        )
        .await
        .unwrap();
    let err = execute_dispatch(&mut client, &proposed, &graph_proposal)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("CAS mismatch") || err.contains("digest mismatch"),
        "{err}"
    );
    assert!(workflow_instances(&mut client).await.is_empty());
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_workflow_dispatch_uses_existing_phase2_proposal_validator() {
    // Behavioral half: a tampered action payload is refused with the canonical
    // Phase 2 digest-mismatch error shape (expected=/provided=/recomputed=),
    // proving the dispatch path runs through the shared proposal validator.
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let (graph_proposal, _mapping) = apply_graph(&mut client).await;
    let proposed = propose_dispatch(&mut client, &graph_proposal).await;
    approve(&mut client, &proposed).await;
    let mut tampered = dispatch_action(&graph_proposal);
    tampered["blocked"][0]["reasons"] = json!(["tampered reason"]);
    let err = client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": proposed["proposal"]["digest"],
                "kind": "product_to_code.dispatch",
                "action": tampered,
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("digest mismatch"), "{err}");
    assert!(err.contains("expected="), "{err}");
    assert!(err.contains("provided="), "{err}");
    assert!(err.contains("recomputed="), "{err}");
    assert!(workflow_instances(&mut client).await.is_empty());
    stop(client, handle).await;

    // Structural half: the daemon dispatch handler wraps the Phase 2 validator
    // entry points instead of duplicating digest logic.
    let server_source =
        std::fs::read_to_string(format!("{}/src/server.rs", env!("CARGO_MANIFEST_DIR"))).unwrap();
    let dispatch_region_start = server_source
        .find("fn handle_product_to_code_dispatch_execute")
        .expect("dispatch execute handler exists");
    let dispatch_region = &server_source[dispatch_region_start..];
    let dispatch_region = &dispatch_region[..dispatch_region
        .find("\n    fn ")
        .unwrap_or(dispatch_region.len())];
    assert!(
        dispatch_region.contains("begin_execute_action"),
        "dispatch executor must reuse the Phase 2 begin_execute_action validator"
    );
    assert!(
        dispatch_region.contains("recompute_proposal_digest"),
        "dispatch executor must reuse the Phase 2 digest recomputation, not duplicate it"
    );
    assert!(
        !dispatch_region.contains("Sha256::digest"),
        "dispatch executor must not hand-roll digest logic"
    );
}

#[tokio::test]
async fn test_daemon_workflow_dispatch_runs_implement_featureset_for_unblocked_tickets_only() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let (graph_proposal, mapping) = apply_graph(&mut client).await;
    let proposed = propose_dispatch(&mut client, &graph_proposal).await;

    // The canonical action resolves graph node ids to the minted TKT ids from
    // the prior approved graph apply execution.
    let canonical = &proposed["proposal"]["action"];
    let expected_ticket = mapping["NODE-contracts"].as_str().unwrap();
    assert!(expected_ticket.starts_with("TKT-"));
    assert_eq!(
        canonical["dispatches"][0]["ticket_id"],
        json!(expected_ticket)
    );
    assert_eq!(
        canonical["dispatches"][0]["workflow"],
        "implement-featureset"
    );
    assert_eq!(canonical["blocked"][0]["graph_node_id"], "NODE-tests");

    approve(&mut client, &proposed).await;
    let executed = execute_dispatch(&mut client, &proposed, &graph_proposal)
        .await
        .unwrap();
    let result = &executed["result"];
    assert_eq!(result["status"], "completed");
    let dispatched = result["dispatched"].as_array().unwrap();
    assert_eq!(dispatched.len(), 1);
    assert_eq!(dispatched[0]["ticket_id"], expected_ticket);
    assert_eq!(dispatched[0]["workflow"], "implement-featureset");

    let instances = workflow_instances(&mut client).await;
    assert_eq!(instances.len(), 1, "{instances:?}");
    assert_eq!(instances[0]["workflow"], "implement-featureset");
    assert_eq!(instances[0]["params"]["taskId"], json!(expected_ticket));
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_workflow_dispatch_rejects_unknown_graph_apply_execution() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let (_graph_proposal, _mapping) = apply_graph(&mut client).await;

    let err = client
        .call(
            "factory.propose_action",
            json!({
                "kind": "product_to_code.dispatch",
                "action": dispatch_action("0000000000000000000000000000000000000000000000000000000000000000"),
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("graph apply") || err.contains("unknown proposal"),
        "{err}"
    );
    assert!(workflow_instances(&mut client).await.is_empty());
    stop(client, handle).await;
}
