//! Phase 5 read-only factory analytics RPC tests: `factory.scorecards` and
//! `factory.recommend`. Verifies envelope shape, structured-source-only
//! normalization, unobserved source families, determinism across repeated
//! calls, and that neither RPC mutates any daemon state.

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
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call("repo.add", json!({"name":"repo-a", "path": repo_dir.path()}))
        .await
        .unwrap();
    (home, repo_dir, layout, handle, client)
}

fn availability_of(resp: &Value, family: &str) -> bool {
    resp["availability"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["source_family"] == json!(family))
        .unwrap_or_else(|| panic!("family {family} missing from availability"))["available"]
        .as_bool()
        .unwrap()
}

#[tokio::test]
async fn factory_scorecards_rpc_returns_read_only_envelope_from_structured_sources() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();

    assert_eq!(resp["schema_version"], json!(1));
    assert_eq!(resp["repo"], json!("repo-a"));
    assert!(resp.get("generated_at").is_some());
    assert!(resp["scorecards"].is_array());
    assert!(resp["source_counts"].is_array());
    assert!(resp["availability"].is_array());
    // AgentRecord/WorkflowInstance are structured RK stores (available even with
    // zero rows); Phase 3/4 families have no RK store yet and are unobserved.
    assert!(availability_of(&resp, "AgentRecord"));
    assert!(availability_of(&resp, "WorkflowInstance"));
    assert!(!availability_of(&resp, "Phase3VerifiedDelivery"));
    assert!(!availability_of(&resp, "Phase4CiSignal"));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_recommend_rpc_returns_advisory_read_only_recommendations() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let resp = client
        .call("factory.recommend", json!({"repo":"repo-a"}))
        .await
        .unwrap();

    assert_eq!(resp["schema_version"], json!(1));
    assert_eq!(resp["nature"], json!("advisory"));
    assert!(resp["recommendations"].is_array());
    assert!(resp["suppressions"].is_array());
    // No mutation-shaped instruction leaks into the advisory payload.
    let blob = resp.to_string();
    for banned in [
        "\"apply\"",
        "\"dispatch\"",
        "rewrite-policy",
        "update-workflow",
        "\"approve\"",
    ] {
        assert!(!blob.contains(banned), "advisory payload must not contain {banned}");
    }

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_rpcs_report_missing_source_families_as_unobserved_not_zero() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let warnings = resp["warnings"].as_array().unwrap();
    for family in [
        "Phase3Contract",
        "Phase3VerifiedDelivery",
        "StructuredReviewerRework",
        "Phase4CiSignal",
        "StructuredRevert",
        "HumanGateDecision",
        "RecurrenceKey",
        "PricingSnapshot",
    ] {
        assert!(!availability_of(&resp, family), "{family} must be unobserved");
        assert!(
            warnings.iter().any(|w| w.as_str().unwrap().contains(family)),
            "warning must name unobserved family {family}"
        );
    }
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_rpcs_are_deterministic_and_read_only_across_repeated_calls() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let first = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let second = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();

    // Everything but the injected generated_at wall clock must match byte-for-byte.
    let strip = |mut v: Value| -> Value {
        if let Some(obj) = v.as_object_mut() {
            obj.remove("generated_at");
        }
        v
    };
    assert_eq!(strip(first), strip(second));

    // The read-only RPCs did not disturb the tuplespace: a following snapshot
    // still reports the same empty agent/workflow rows.
    let snapshot = client
        .call("factory.snapshot", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    assert_eq!(snapshot["snapshot"]["agents"], json!([]));
    assert_eq!(snapshot["snapshot"]["workflows"], json!([]));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
