mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::{path::Path, process::Command};
use support::connect;

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

fn repository() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("rk-ticket-graph-apply")
        .tempdir()
        .unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "test@example.com"]);
    git(dir.path(), &["config", "user.name", "Test"]);
    std::fs::write(dir.path().join("README.md"), "# Fixture\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(dir.path(), &["commit", "-m", "initial"]);
    dir
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

fn initiative() -> Value {
    json!({
        "id": "INIT-product-to-code",
        "title": "Product to code contracts",
        "scope": "offline-contracts",
        "browser_acceptance_applicable": false,
        "acceptance_criteria": [
            {"id": "AC-1", "text": "Validate graph", "browser_acceptance_applicable": false},
            {"id": "AC-2", "text": "Apply graph", "browser_acceptance_applicable": false}
        ]
    })
}

fn graph() -> Value {
    json!({
        "id": "GRAPH-product-to-code",
        "initiative_id": "INIT-product-to-code",
        "nodes": [
            {"id": "NODE-contracts", "title": "Contracts", "description": "Add contracts", "acceptance_criterion_ids": ["AC-1"]},
            {"id": "NODE-tests", "title": "Tests", "description": "Add tests", "acceptance_criterion_ids": ["AC-2"]}
        ],
        "edges": [
            {"from": "NODE-contracts", "to": "NODE-tests", "relationship": "depends_on"}
        ]
    })
}

fn action() -> Value {
    json!({"repo": "fixture", "graph": graph(), "initiative": initiative()})
}

async fn propose(client: &mut Client) -> Value {
    client
        .call(
            "factory.propose_action",
            json!({"kind": "ticket_graph.apply", "action": action()}),
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

async fn execute(client: &mut Client, proposed: &Value) -> rk_core::Result<Value> {
    client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": proposed["proposal"]["digest"],
                "kind": "ticket_graph.apply",
                "action": action(),
            }),
        )
        .await
}

async fn tickets(client: &mut Client) -> Vec<Value> {
    client
        .call("ticket.list", json!({"scope": "fixture"}))
        .await
        .unwrap()["tickets"]
        .as_array()
        .unwrap()
        .clone()
}

async fn stop(mut client: Client, handle: tokio::task::JoinHandle<rk_core::Result<()>>) {
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_requires_authenticated_operator_approval() {
    let (_home, _repo, layout, handle, mut operator) = setup().await;
    let proposed = propose(&mut operator).await;
    approve(&mut operator, &proposed).await;
    let mut rat = Client::connect_as(&layout, "rat-a").await.unwrap();

    let err = execute(&mut rat, &proposed).await.unwrap_err().to_string();
    assert!(
        err.contains("forbidden") || err.contains("unauthorized"),
        "{err}"
    );
    assert!(tickets(&mut operator).await.is_empty());
    stop(operator, handle).await;
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_rejects_unapproved_status() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let proposed = propose(&mut client).await;

    let err = execute(&mut client, &proposed)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("not approved") || err.contains("forbidden"),
        "{err}"
    );
    assert!(tickets(&mut client).await.is_empty());
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_rejects_digest_mismatch() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let proposed = propose(&mut client).await;
    approve(&mut client, &proposed).await;

    let err = client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposed["proposal"]["id"],
                "digest": "0".repeat(64),
                "kind": "ticket_graph.apply",
                "action": action(),
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("digest mismatch") || err.contains("forbidden"),
        "{err}"
    );
    assert!(err.contains("expected="), "{err}");
    assert!(err.contains("provided="), "{err}");
    assert!(err.contains("recomputed="), "{err}");
    assert!(tickets(&mut client).await.is_empty());
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_rejects_cas_mismatch() {
    let (_home, repo, _layout, handle, mut client) = setup().await;
    let proposed = propose(&mut client).await;
    approve(&mut client, &proposed).await;
    std::fs::write(repo.path().join("CHANGED.md"), "changed\n").unwrap();
    git(repo.path(), &["add", "."]);
    git(repo.path(), &["commit", "-m", "advance"]);

    let err = execute(&mut client, &proposed)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("CAS mismatch") || err.contains("digest mismatch"),
        "{err}"
    );
    assert!(tickets(&mut client).await.is_empty());
    stop(client, handle).await;

    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let proposed = propose(&mut client).await;
    approve(&mut client, &proposed).await;
    client
        .call(
            "ticket.new",
            json!({"title": "Concurrent work", "scope": "fixture"}),
        )
        .await
        .unwrap();

    let err = execute(&mut client, &proposed)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("CAS mismatch") || err.contains("digest mismatch"),
        "{err}"
    );
    assert_eq!(tickets(&mut client).await.len(), 1);
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_creates_tickets_topologically_then_edges() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let proposed = propose(&mut client).await;
    approve(&mut client, &proposed).await;

    let executed = execute(&mut client, &proposed).await.unwrap();
    let result = &executed["result"];
    assert_eq!(result["status"], "completed");
    assert_eq!(result["graph_id"], "GRAPH-product-to-code");
    assert_eq!(result["created_ticket_ids"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["created_dependency_edges"].as_array().unwrap().len(),
        1
    );
    let contracts = result["graph_node_to_ticket_id"]["NODE-contracts"]
        .as_str()
        .unwrap();
    let tests = result["graph_node_to_ticket_id"]["NODE-tests"]
        .as_str()
        .unwrap();
    assert!(contracts.starts_with("TKT-"));
    assert!(tests.starts_with("TKT-"));
    assert_eq!(result["created_ticket_ids"], json!([contracts, tests]));

    let listed = tickets(&mut client).await;
    let test_ticket = listed
        .iter()
        .find(|ticket| ticket["identity"] == tests)
        .unwrap();
    assert_eq!(test_ticket["payload"]["depends_on"], json!([contracts]));
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_idempotent_replay_does_not_duplicate_tickets_or_edges() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let proposed = propose(&mut client).await;
    approve(&mut client, &proposed).await;
    let first = execute(&mut client, &proposed).await.unwrap();
    let second = execute(&mut client, &proposed).await.unwrap();

    assert_eq!(
        first["result"]["execution_id"],
        second["result"]["execution_id"]
    );
    assert_eq!(second["result"]["idempotent_replay"], true);
    assert_eq!(tickets(&mut client).await.len(), 2);
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_concurrent_retries_share_one_execution() {
    let (_home, _repo, layout, handle, mut client) = setup().await;
    let proposed = propose(&mut client).await;
    approve(&mut client, &proposed).await;
    let mut first_client = connect(&layout).await;
    let mut second_client = connect(&layout).await;

    let (first, second) = tokio::join!(
        execute(&mut first_client, &proposed),
        execute(&mut second_client, &proposed),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        first["result"]["execution_id"],
        second["result"]["execution_id"]
    );
    assert_eq!(tickets(&mut client).await.len(), 2);
    stop(client, handle).await;
}

#[tokio::test]
async fn test_daemon_ticket_graph_apply_distinguishes_graph_node_ids_from_tkt_ids() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let mut invalid = graph();
    invalid["nodes"][0]["id"] = json!("TKT-forged");

    let err = client
        .call(
            "factory.propose_action",
            json!({
                "kind": "ticket_graph.apply",
                "action": {"repo": "fixture", "graph": invalid, "initiative": initiative()}
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("TKT-") || err.contains("graph node"), "{err}");
    assert!(tickets(&mut client).await.is_empty());
    stop(client, handle).await;
}
