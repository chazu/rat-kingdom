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
        .prefix("rk-ticket-graph-daemon")
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
    (home, repo, handle, client)
}

fn initiative() -> Value {
    json!({
        "id": "INIT-product-to-code",
        "title": "Product to code contracts",
        "scope": "offline-contracts",
        "browser_acceptance_applicable": true,
        "acceptance_criteria": [
            {"id": "AC-1", "text": "Validate graph", "browser_acceptance_applicable": false},
            {"id": "AC-2", "text": "Propose apply", "browser_acceptance_applicable": true}
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
        "edges": [{"from": "NODE-contracts", "to": "NODE-tests", "relationship": "depends_on"}]
    })
}

async fn propose(client: &mut Client, action: Value) -> rk_core::Result<Value> {
    client
        .call(
            "factory.propose_action",
            json!({"kind": "ticket_graph.apply", "action": action}),
        )
        .await
}

#[tokio::test]
async fn test_ticket_graph_apply_recomputes_typed_apply_plan_in_daemon() {
    let (_home, _repo, handle, mut client) = setup().await;

    let proposed = propose(
        &mut client,
        json!({"repo": "fixture", "graph": graph(), "initiative": initiative()}),
    )
    .await
    .unwrap();

    let action = &proposed["proposal"]["action"];
    assert_eq!(action["kind"], "ticket_graph.apply");
    assert!(action.get("topological_order").is_none());
    assert!(action.get("mutations").is_none());
    assert_eq!(
        action["apply_plan"]["topological_order"],
        json!(["NODE-contracts", "NODE-tests"])
    );
    assert_eq!(
        action["apply_plan"]["creates"][0]["stable_graph_node_id"],
        "NODE-contracts"
    );
    assert_eq!(
        action["apply_plan"]["dependencies"][0]["blocked_graph_node_id"],
        "NODE-tests"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_ticket_graph_apply_rejects_extra_or_tampered_derived_fields() {
    let (_home, _repo, handle, mut client) = setup().await;
    let err = propose(
        &mut client,
        json!({
            "repo": "fixture",
            "graph": graph(),
            "initiative": initiative(),
            "topological_order": ["NODE-tests"],
            "mutations": [{"operation": "ticket.create", "stable_graph_node_id": "NODE-forged"}]
        }),
    )
    .await
    .unwrap_err()
    .to_string();

    assert!(
        err.contains("unknown field") || err.contains("invalid ticket_graph.apply"),
        "{err}"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_ticket_graph_apply_rejects_initiative_id_mismatch() {
    let (_home, _repo, handle, mut client) = setup().await;
    let mut mismatched = initiative();
    mismatched["id"] = json!("INIT-other");

    let err = propose(
        &mut client,
        json!({"repo": "fixture", "graph": graph(), "initiative": mismatched}),
    )
    .await
    .unwrap_err()
    .to_string();

    assert!(
        err.contains(
            "graph initiative_id INIT-product-to-code must match initiative id INIT-other"
        ),
        "{err}"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_ticket_graph_apply_rejects_malformed_missing_and_cyclic_graphs() {
    let (_home, _repo, handle, mut client) = setup().await;

    let missing = propose(
        &mut client,
        json!({"repo": "fixture", "graph": {"id": "GRAPH-bad"}, "initiative": initiative()}),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        missing.contains("missing field") || missing.contains("invalid ticket_graph.apply"),
        "{missing}"
    );

    let mut cyclic = graph();
    cyclic["edges"] = json!([
        {"from": "NODE-contracts", "to": "NODE-tests", "relationship": "depends_on"},
        {"from": "NODE-tests", "to": "NODE-contracts", "relationship": "depends_on"}
    ]);
    let cycle = propose(
        &mut client,
        json!({"repo": "fixture", "graph": cyclic, "initiative": initiative()}),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        cycle.contains("cycle path NODE-contracts -> NODE-tests -> NODE-contracts"),
        "{cycle}"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
