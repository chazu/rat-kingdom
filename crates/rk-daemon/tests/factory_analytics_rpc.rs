//! Phase 5 read-only factory analytics RPC tests: `factory.scorecards` and
//! `factory.recommend`. Verifies envelope shape, structured-source-only
//! normalization, unobserved source families, determinism across repeated
//! calls, and that neither RPC mutates any daemon state.

mod fixture;

use chrono::{TimeZone, Utc};
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::{path::Path, process::Command, time::Duration};

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

fn fixed_clock() -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 123_000_000).unwrap()
}

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
    let wf_dir = repo_dir.path().join(".rk/workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("factory-test.cue"), WORKFLOW).unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_request_clock_for_tests(fixed_clock);
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

async fn run_factory_workflow(client: &mut Client, task: &str) {
    let proposed = client
        .call(
            "factory.propose_action",
            json!({
                "kind":"workflow.run",
                "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":task}, "coordinator":"coord-a"}
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
    client
        .call(
            "factory.execute_action",
            json!({
                "proposal_id": proposal_id,
                "digest": digest,
                "action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":task}, "coordinator":"coord-a"}
            }),
        )
        .await
        .unwrap();
}

async fn snapshot_state(client: &mut Client) -> Value {
    client
        .call("factory.snapshot", json!({"repo":"repo-a", "include_archived":true}))
        .await
        .unwrap()["snapshot"]
        .clone()
}

async fn assert_bad_params(client: &mut Client, params: Value, expected: &str) {
    let err = client
        .call("factory.scorecards", params)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains(expected), "{err:?} did not contain {expected:?}");
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
    // These families have structured RK stores and are available even with zero rows.
    assert!(availability_of(&resp, "AgentRecord"));
    assert!(availability_of(&resp, "WorkflowInstance"));
    assert!(availability_of(&resp, "Phase4CiSignal"));
    assert!(availability_of(&resp, "HumanGateDecision"));
    assert!(availability_of(&resp, "RecurrenceKey"));
    // These source families have no structured RK store yet and are unobserved.
    assert!(!availability_of(&resp, "Phase3Contract"));
    assert!(!availability_of(&resp, "Phase3VerifiedDelivery"));
    assert!(!availability_of(&resp, "StructuredReviewerRework"));
    assert!(!availability_of(&resp, "StructuredRevert"));
    assert!(!availability_of(&resp, "PricingSnapshot"));

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
        "StructuredRevert",
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
async fn factory_rpcs_reject_invalid_read_only_params() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;

    assert_bad_params(&mut client, json!({}), "repo is required").await;
    assert_bad_params(&mut client, json!({"repo":"   "}), "repo is required").await;
    assert_bad_params(
        &mut client,
        json!({"repo":"repo-a", "group_by":"harness_model"}),
        "unsupported group_by",
    )
    .await;
    assert_bad_params(
        &mut client,
        json!({"repo":"repo-a", "since":20, "until":10}),
        "since must be <= until",
    )
    .await;

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_rpcs_are_deterministic_and_read_only_across_repeated_calls() {
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    run_factory_workflow(&mut client, "one").await;
    let before = snapshot_state(&mut client).await;
    let has_nonempty_state = before["agents"].as_array().unwrap().len()
        + before["workflows"].as_array().unwrap().len()
        + before["approvals"]["proposals"].as_array().unwrap().len()
        + before["approvals"]["grants"].as_array().unwrap().len()
        > 0;
    assert!(has_nonempty_state, "read-only proof must compare nonempty factory state");

    let first = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let second = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();

    assert_eq!(first["generated_at"], json!("2023-11-14T22:13:20.123Z"));
    assert_eq!(first, second, "fixed clock makes the whole RPC response deterministic");

    let recommend = client
        .call("factory.recommend", json!({"repo":"repo-a", "min_sample":1000}))
        .await
        .unwrap();
    assert!(recommend["recommendations"].as_array().unwrap().iter().all(|r| {
        r["advice"].is_null() || r["suppressed"] == json!(true)
    }));
    if !recommend["recommendations"].as_array().unwrap().is_empty() {
        assert!(recommend["suppressions"].as_array().unwrap().iter().any(|s| {
            s["reason"] == json!("LowSample")
        }));
    }

    let after = snapshot_state(&mut client).await;
    assert_eq!(before, after, "scorecards/recommend must not mutate factory state");

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
