//! Phase 5 read-only factory analytics RPC tests: `factory.scorecards` and
//! `factory.recommend`. Verifies envelope shape, structured-source-only
//! normalization, unobserved source families, determinism across repeated
//! calls, and that neither RPC mutates any daemon state.

mod fixture;

use chrono::{TimeZone, Utc};
mod support;

use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Tuple};
use rk_daemon::{Client, Daemon};
use rk_space::Space;
use serde_json::{json, Value};
use std::{path::Path, process::Command, time::Duration};
use support::connect;

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

/// `RK_FAKE_HARNESS_CMD` is process-global; the native_recorded_cost journey
/// tests below vary it per-spawn within one test (a plain success, a crash,
/// a distinct declared cost) so a concurrently-running test in this same
/// binary overwriting it mid-spawn would corrupt an in-flight generation's
/// reported cost. Every test in this file holds this lock for its full
/// duration — matching the established `HARNESS_ENV_LOCK` convention used
/// wherever `RK_FAKE_HARNESS_CMD` is mutated (e.g. `agent_lifecycle.rs`,
/// `automated_landing.rs`) — even the ones that only ever set it to the same
/// `WORKING_FAKE` value, since a single unguarded test in the file would
/// reopen the race for everyone else.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Layout,
    tokio::task::JoinHandle<rk_core::Result<()>>,
    Client,
) {
    let (home, repo_dir, layout, handle, _space, client) = setup_with_space().await;
    (home, repo_dir, layout, handle, client)
}

async fn setup_with_space() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Layout,
    tokio::task::JoinHandle<rk_core::Result<()>>,
    Space,
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
    support::install_default_repository_policy(repo_dir.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        rk_ledger::Budget::default(),
        space.clone(),
    )
    .unwrap();
    daemon.set_request_clock_for_tests(fixed_clock);
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name":"repo-a", "path": repo_dir.path()}),
        )
        .await
        .unwrap();
    (home, repo_dir, layout, handle, space, client)
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

fn source_count<'a>(resp: &'a Value, family: &str) -> &'a Value {
    resp["source_counts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["source_family"] == json!(family))
        .unwrap_or_else(|| panic!("missing source count for {family}"))
}

fn ci_event(delivery_id: &str, kind: &str, at: chrono::DateTime<Utc>) -> Tuple {
    let mut tuple = Tuple::new(
        Category::Event,
        "ci",
        format!("sdlc:event:github:{delivery_id}"),
        "source:github",
        json!({
            "source": "github",
            "delivery_id": delivery_id,
            "family": "ci",
            "subject": "repo-a:ci:build",
            "kind": kind,
            "summary": "structured event only",
            "observed_at": at.to_rfc3339(),
            "occurred_at": at.to_rfc3339(),
            "correlation": {"repo": "repo-a", "workflow": "build", "commit_sha": "abc123"},
            "payload": {"type": "ci", "status": "completed", "conclusion": "success"}
        }),
    );
    tuple.created_at = at;
    tuple
}

/// A genuine `landing_processed` marker matching the exact producer schema
/// `landing::LandingPipeline::mark_processed` writes (module doc there): same
/// category/identity/instance and field set, so this exercises the real
/// `native_delivery` provenance check end-to-end rather than a stand-in shape.
#[allow(clippy::too_many_arguments)]
fn landing_processed_marker(
    repo: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    task: &str,
    outcome: &str,
    target_head: Option<&str>,
    at: chrono::DateTime<Utc>,
) -> Tuple {
    let mut tuple = Tuple::new(
        Category::Event,
        repo,
        "landing_processed",
        "daemon",
        json!({
            "branch": branch,
            "target": target,
            "target_head": target_head,
            "head_sha": head_sha,
            "task": task,
            "outcome": outcome,
            "admission_hold": Value::Null,
            "admission_recovery": Value::Null,
        }),
    )
    .with_lifecycle(Lifecycle::Furniture);
    tuple.created_at = at;
    tuple
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

async fn settled_agent_name(client: &mut Client) -> String {
    for _ in 0..100 {
        if let Some(agent) = client
            .call("agent.list", json!({"include_archived":true}))
            .await
            .unwrap()["agents"]
            .as_array()
            .and_then(|agents| agents.first())
            .filter(|agent| {
                matches!(
                    agent["state"].as_str(),
                    Some("completed") | Some("failed") | Some("dismissed")
                )
            })
        {
            return agent["name"].as_str().unwrap().to_owned();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("workflow must leave a settled structured agent record");
}

async fn snapshot_state(client: &mut Client) -> Value {
    client
        .call(
            "factory.snapshot",
            json!({"repo":"repo-a", "include_archived":true}),
        )
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
    assert!(
        err.contains(expected),
        "{err:?} did not contain {expected:?}"
    );
}

#[tokio::test]
async fn factory_scorecards_rpc_returns_read_only_envelope_from_structured_sources() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
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
    assert!(availability_of(&resp, "StructuredReviewerRework"));
    assert!(availability_of(&resp, "StructuredRevert"));
    assert!(!availability_of(&resp, "PricingSnapshot"));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_recommend_rpc_returns_advisory_read_only_recommendations() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
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
        assert!(
            !blob.contains(banned),
            "advisory payload must not contain {banned}"
        );
    }

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_rpcs_report_missing_source_families_as_unobserved_not_zero() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let warnings = resp["warnings"].as_array().unwrap();
    for family in [
        "Phase3Contract",
        "Phase3VerifiedDelivery",
        "PricingSnapshot",
    ] {
        assert!(
            !availability_of(&resp, family),
            "{family} must be unobserved"
        );
        assert!(
            warnings
                .iter()
                .any(|w| w.as_str().unwrap().contains(family)),
            "warning must name unobserved family {family}"
        );
    }
    for family in ["StructuredReviewerRework", "StructuredRevert"] {
        assert!(
            availability_of(&resp, family),
            "{family} has a daemon read seam"
        );
        assert_eq!(source_count(&resp, family)["event_count"], json!(0));
        assert!(warnings
            .iter()
            .all(|w| !w.as_str().unwrap().contains(family)));
    }
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_analytics_reads_revert_fact_and_rework_verdict_end_to_end() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo_dir, _layout, handle, space, mut client) = setup_with_space().await;
    run_factory_workflow(&mut client, "structured-outcomes").await;
    let agent_name = settled_agent_name(&mut client).await;
    // Production revert/verdict producers use the agent's registered scope,
    // including when the checkout basename differs from the registration.
    let agent = client
        .call("agent.status", json!({"name": agent_name}))
        .await
        .unwrap();
    let repo_scope = agent["agent"]["repo_name"].as_str().unwrap().to_string();
    assert_eq!(repo_scope, "repo-a");

    space
        .out(Tuple::new(
            Category::Fact,
            &repo_scope,
            format!("merge-reverted-{agent_name}"),
            "test-castle",
            json!({
                "agent": agent_name,
                "task": "TKT-REVERT",
                "merge_commit": "merge-abc",
                "revert_commit": "revert-def",
                "detail": "ignored prose"
            }),
        ))
        .unwrap();
    space
        .out(Tuple::new(
            Category::Artifact,
            &repo_scope,
            "review",
            "test-castle",
            json!({
                "agent": agent_name,
                "task": "TKT-REWORK",
                "recommendation": "REWORK",
                "notes": "ignored prose"
            }),
        ))
        .unwrap();

    let first = client
        .call("factory.scorecards", json!({"repo":repo_scope.clone()}))
        .await
        .unwrap();
    let second = client
        .call("factory.scorecards", json!({"repo":repo_scope.clone()}))
        .await
        .unwrap();
    assert_eq!(
        first, second,
        "same daemon state must serialize identically"
    );
    assert!(availability_of(&first, "StructuredRevert"));
    assert!(availability_of(&first, "StructuredReviewerRework"));
    assert_eq!(
        source_count(&first, "StructuredRevert")["active_source_count"],
        json!(1)
    );
    assert_eq!(
        source_count(&first, "StructuredRevert")["event_count"],
        json!(1)
    );
    assert_eq!(
        source_count(&first, "StructuredReviewerRework")["active_source_count"],
        json!(1)
    );
    assert_eq!(
        source_count(&first, "StructuredReviewerRework")["event_count"],
        json!(1)
    );

    let row = first["scorecards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| !row["projected"].as_bool().unwrap_or(false))
        .unwrap();
    assert_eq!(row["metrics"]["reverted"], json!(1));
    assert_eq!(row["metrics"]["revert_sample_size"], json!(1));
    assert_eq!(row["metrics"]["reworked"], json!(1));
    assert_eq!(row["metrics"]["rework_sample_size"], json!(1));

    let recommend = client
        .call("factory.recommend", json!({"repo":repo_scope}))
        .await
        .unwrap();
    for rule in ["reverts", "high_rework"] {
        let recommendation = recommend["recommendations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|recommendation| recommendation["rule"] == json!(rule))
            .unwrap_or_else(|| panic!("missing recommendation for {rule}"));
        assert!(recommendation["suppressed"] == json!(true));
        assert_eq!(recommendation["suppression_reason"], json!("low_sample"));
        assert_eq!(recommendation["evidence"]["denominator"], json!(1));
    }

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_rpcs_reject_invalid_read_only_params() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
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
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, mut client) = setup().await;
    run_factory_workflow(&mut client, "one").await;
    let before = snapshot_state(&mut client).await;
    let has_nonempty_state = before["agents"].as_array().unwrap().len()
        + before["workflows"].as_array().unwrap().len()
        + before["approvals"]["proposals"].as_array().unwrap().len()
        + before["approvals"]["grants"].as_array().unwrap().len()
        > 0;
    assert!(
        has_nonempty_state,
        "read-only proof must compare nonempty factory state"
    );

    let first = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let second = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();

    assert_eq!(first["generated_at"], json!("2023-11-14T22:13:20.123Z"));
    assert_eq!(
        first, second,
        "fixed clock makes the whole RPC response deterministic"
    );

    let recommend = client
        .call(
            "factory.recommend",
            json!({"repo":"repo-a", "min_sample":1000}),
        )
        .await
        .unwrap();
    assert!(recommend["recommendations"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| { r["advice"].is_null() || r["suppressed"] == json!(true) }));
    if !recommend["recommendations"].as_array().unwrap().is_empty() {
        assert!(recommend["suppressions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| { s["reason"] == json!("LowSample") }));
    }

    let after = snapshot_state(&mut client).await;
    assert_eq!(
        before, after,
        "scorecards/recommend must not mutate factory state"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn factory_analytics_scorecards_windows_workflow_approval_and_ci_sources() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, space, mut client) = setup_with_space().await;
    run_factory_workflow(&mut client, "windowed").await;
    client
        .call(
            "ticket.new",
            json!({"title":"recurrent defect", "scope":"repo-a", "coalesce_key":"factory-analytics-window"}),
        )
        .await
        .unwrap();
    space
        .out(ci_event("failed", "ci_failed", fixed_clock()))
        .unwrap();
    space
        .out(ci_event(
            "recovered",
            "ci_recovered",
            fixed_clock() + chrono::Duration::seconds(1),
        ))
        .unwrap();

    let all = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    assert_eq!(source_count(&all, "RecurrenceKey")["event_count"], json!(1));
    assert_eq!(
        source_count(&all, "Phase4CiSignal")["event_count"],
        json!(2)
    );

    let future_since = (Utc::now() + chrono::Duration::days(1)).timestamp_millis();
    let future = client
        .call(
            "factory.scorecards",
            json!({"repo":"repo-a", "since": future_since}),
        )
        .await
        .unwrap();
    for family in [
        "AgentRecord",
        "WorkflowInstance",
        "HumanGateDecision",
        "RecurrenceKey",
        "Phase4CiSignal",
    ] {
        assert_eq!(
            source_count(&future, family)["event_count"],
            json!(0),
            "{family} must honor since"
        );
    }

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

fn native_delivery(resp: &Value) -> &Value {
    &resp["native_delivery"]
}

/// End-to-end boundary journey for the additive `native_delivery` section
/// (P9.1): a genuine stored `"landed"` marker and a genuine stored
/// `"gate-held"` marker for a DIFFERENT work key, read through the real
/// `factory.scorecards` RPC and `Server::factory_analytics_inputs`'s bounded
/// storage query — not the pure reducer directly. Also proves repo scoping
/// (a marker written under a different repo scope never appears) and the
/// available-but-empty distinction (a repo with zero markers). Detailed
/// conflict/malformed-record cases are exhaustively covered at the pure
/// `factory_analytics` unit-test layer and are not repeated here.
#[tokio::test]
async fn factory_analytics_native_delivery_reads_stored_markers_end_to_end_and_scopes_by_repo() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, space, mut client) = setup_with_space().await;
    let at = fixed_clock();

    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-landed",
            "sha-landed",
            "main",
            "TKT-LANDED",
            "landed",
            Some("merge-abc"),
            at,
        ))
        .unwrap();
    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-held",
            "sha-held",
            "main",
            "TKT-HELD",
            "gate-held",
            None,
            at + chrono::Duration::seconds(1),
        ))
        .unwrap();
    // A marker under a DIFFERENT repo scope must never be visible under
    // "repo-a" — proves the storage-side `Pattern::scope` fence, not just an
    // in-process filter the pure reducer could get wrong.
    space
        .out(landing_processed_marker(
            "repo-other",
            "feature-cross-repo",
            "sha-cross-repo",
            "main",
            "TKT-CROSS",
            "landed",
            Some("merge-cross"),
            at,
        ))
        .unwrap();

    let before = snapshot_state(&mut client).await;

    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let nd = native_delivery(&resp);
    assert_eq!(nd["available"], json!(true));
    assert_eq!(
        nd["coverage"]["scanned"],
        json!(2),
        "only repo-a's own two markers"
    );
    assert_eq!(nd["coverage"]["may_hide_delivery"], json!(false));
    assert_eq!(nd["delivered_edges"], json!(1));
    assert_eq!(nd["delivered_tasks"], json!(1));
    assert_eq!(nd["no_delivery_observed"]["gate_held"], json!(1));
    assert_eq!(nd["observed_incidents"]["gate_held"], json!(1));
    assert_eq!(nd["unknown"]["malformed"], json!(0));

    // Available-but-empty: a repo with a real (successful) read and zero
    // matching markers must render real zeros, not `available:false`.
    let empty = client
        .call(
            "factory.scorecards",
            json!({"repo":"repo-with-no-landings"}),
        )
        .await
        .unwrap();
    let nd_empty = native_delivery(&empty);
    assert_eq!(nd_empty["available"], json!(true));
    assert_eq!(nd_empty["coverage"]["scanned"], json!(0));
    assert_eq!(nd_empty["delivered_edges"], json!(0));

    let after = snapshot_state(&mut client).await;
    assert_eq!(
        before, after,
        "reading native_delivery through factory.scorecards must not mutate factory state"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

/// A requested `since`/`until` window can exclude the one `"landed"` marker
/// for a work key while a non-delivery marker for that same key stays inside
/// the window — proven here with a real stored pair through the real RPC,
/// not simulated in the pure reducer. `no_delivery_observed` must still
/// render for the in-window marker, but `coverage.may_hide_delivery` must
/// say so explicitly rather than let the count read as an absolute claim.
#[tokio::test]
async fn factory_analytics_native_delivery_window_can_hide_a_later_delivery() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, space, mut client) = setup_with_space().await;
    let gate_held_at = fixed_clock();
    let landed_at = fixed_clock() + chrono::Duration::days(1);

    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-recovered",
            "sha-recovered",
            "main",
            "TKT-RECOVERED",
            "gate-held",
            None,
            gate_held_at,
        ))
        .unwrap();
    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-recovered",
            "sha-recovered",
            "main",
            "TKT-RECOVERED",
            "landed",
            Some("merge-recovered"),
            landed_at,
        ))
        .unwrap();

    // Unwindowed: both markers observed, key resolves to delivered.
    let unwindowed = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let nd_all = native_delivery(&unwindowed);
    assert_eq!(nd_all["delivered_edges"], json!(1));
    assert_eq!(nd_all["no_delivery_observed"]["gate_held"], json!(0));
    assert_eq!(nd_all["coverage"]["may_hide_delivery"], json!(false));

    // Windowed to exclude the later landed marker: the same key now shows
    // no delivery WITHIN this coverage, and that must be flagged, not
    // silently reported as if it were an absolute "never landed" fact.
    let until = (landed_at - chrono::Duration::hours(1)).timestamp_millis();
    let windowed = client
        .call(
            "factory.scorecards",
            json!({"repo":"repo-a", "until": until}),
        )
        .await
        .unwrap();
    let nd_windowed = native_delivery(&windowed);
    assert_eq!(nd_windowed["delivered_edges"], json!(0));
    assert_eq!(nd_windowed["no_delivery_observed"]["gate_held"], json!(1));
    assert_eq!(nd_windowed["coverage"]["may_hide_delivery"], json!(true));
    assert!(nd_windowed["warnings"].as_array().unwrap().iter().any(|w| w
        .as_str()
        .unwrap()
        .contains("no_delivery_observed_is_coverage_relative")));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

/// The storage-side cap this section relies on (`Server::
/// factory_analytics_inputs`'s `scan_newest_limited(..., MAX_SCAN_TUPLES+1)`)
/// exercised at the real layer that queries it, not asserted only against a
/// hand-built `NativeDeliveryInputs` in the pure unit tests. Writes one more
/// distinct-work-key `"landed"` marker than the daemon's fixed 10_000-row
/// cap and proves the RPC reports the exact bound and truncation honestly.
#[tokio::test]
async fn factory_analytics_native_delivery_truncates_at_the_real_storage_cap() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, space, mut client) = setup_with_space().await;
    let at = fixed_clock();
    // Matches the daemon's private `server::MAX_SCAN_TUPLES` (10_000); kept
    // as a literal here since that constant is not exported across the
    // crate boundary, mirroring this codebase's existing convention of
    // hardcoding the `"landing_processed"` identity in integration tests.
    const MAX_SCAN_TUPLES: usize = 10_000;
    for i in 0..=MAX_SCAN_TUPLES {
        space
            .out(landing_processed_marker(
                "repo-a",
                &format!("feature-{i}"),
                &format!("sha-{i}"),
                "main",
                "",
                "landed",
                Some(&format!("merge-{i}")),
                at,
            ))
            .unwrap();
    }

    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let nd = native_delivery(&resp);
    assert_eq!(nd["coverage"]["limit"], json!(MAX_SCAN_TUPLES));
    assert_eq!(nd["coverage"]["scanned"], json!(MAX_SCAN_TUPLES));
    assert_eq!(nd["coverage"]["truncated"], json!(true));
    assert_eq!(nd["coverage"]["may_hide_delivery"], json!(true));
    assert!(nd["warnings"].as_array().unwrap().iter().any(|w| w
        .as_str()
        .unwrap()
        .contains("native_delivery_coverage_truncated")));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

// -- native_recorded_cost (P9.2 correction, TKT-zabok-huzab-vakot): real
// end-to-end journey through agent.spawn/agent.respawn/agent.archive and
// genuine landing_processed/resubmission markers, not the pure reducer
// directly (that is exhaustively covered at the factory_analytics unit-test
// layer). ------------------------------------------------------------------

fn native_recorded_cost(resp: &Value) -> &Value {
    &resp["native_recorded_cost"]
}

const CRASHING_FAKE: &str = "read -r _prompt; exit 3";

/// A fake harness reporting an exact, caller-chosen `total_cost_usd` so each
/// generation in a journey test can carry a distinct, assertable cost.
fn cost_fake(total_cost_usd: &str) -> String {
    fixture::with_rk_done(&format!(
        r#"
read -r _prompt
echo '{{"type":"system","subtype":"init","session_id":"wf-fake"}}'
rk_done "work done"
echo '{{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"wf-fake","total_cost_usd":{total_cost_usd},"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#
    ))
}

fn resubmission_marker(
    repo: &str,
    identity: &str,
    rework_ticket: &str,
    original_task: &str,
    at: chrono::DateTime<Utc>,
) -> Tuple {
    let mut tuple = Tuple::new(
        Category::Event,
        repo,
        identity,
        "daemon",
        json!({
            "dispatch_key": "dk-1",
            "rework_ticket": rework_ticket,
            "rework_branch": "rework-branch",
            "branch": "feature",
            "target": "main",
            "task": original_task,
            "head_sha": "resolved-sha",
            "seq": 1,
            "state": "queued",
        }),
    )
    .with_lifecycle(Lifecycle::Furniture);
    tuple.created_at = at;
    tuple
}

/// Spawn directly via `agent.spawn` (not a workflow), so `task`/`review` are
/// exactly the caller's values — real `ReviewContext` binding is supported
/// natively by `SpawnParams.review` (`crates/rk-daemon/src/supervisor.rs`),
/// the same field a landing-pipeline review dispatch sets, not a test hack.
async fn spawn(client: &mut Client, repo: &str, task: &str, review: Option<Value>) -> String {
    let mut params = json!({"repo": repo, "task": task, "harness": "fake"});
    if let Some(review) = review {
        params["review"] = review;
    }
    let spawned = client.call("agent.spawn", params).await.unwrap();
    spawned["agent"]["name"].as_str().unwrap().to_string()
}

async fn wait_for_state(client: &mut Client, name: &str, want: &[&str]) -> Value {
    for _ in 0..250 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if let Some(state) = status["agent"]["state"].as_str() {
            if want.contains(&state) {
                return status["agent"].clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("agent {name} never reached one of {want:?}");
}

/// Reducer/registry-level coverage over SEEDED evidence, not a claim that a
/// source was actually landed by the real `LandingPipeline`: the
/// `landing_processed`/`landing_rework_resubmission` markers are written
/// directly via `space.out` (matching the producer's exact schema, per
/// `landing_processed_marker`'s doc, but not produced by that producer), and
/// the reviewer's `ReviewContext` is passed straight to a generic
/// `agent.spawn` rather than assigned by the pipeline's own
/// `dispatch_review`. This is deliberate: it isolates the join/dedup/
/// malformed-cost logic from the much larger real landing+review machinery,
/// which the ONE genuine journey below
/// (`factory_analytics_native_recorded_cost_genuine_pipeline_journey_end_to_end`)
/// exercises instead. Covers: a real settled `AgentRecord`'s recorded
/// implementation cost joined to a seeded delivery, a seeded reviewer
/// binding, a correction generation for TKT-1 that is ALSO itself seeded as
/// independently delivered TKT-2 (proving the cross-task dedup fix so the
/// repo-wide total counts its cost once, not twice), and a malformed
/// (negative) harness-reported cost — with a read-only invariant check.
#[tokio::test]
async fn factory_analytics_native_recorded_cost_reads_real_delivery_review_correction_and_malformed_cost_end_to_end(
) {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, repo_dir, _layout, handle, space, mut client) = setup_with_space().await;
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    let at = fixed_clock();

    std::env::set_var("RK_FAKE_HARNESS_CMD", cost_fake("0.10"));
    let impl_a = spawn(&mut client, &repo_path, "TKT-1", None).await;
    wait_for_state(&mut client, &impl_a, &["completed"]).await;
    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-a",
            "sha-a",
            "main",
            "TKT-1",
            "landed",
            Some("merge-a"),
            at,
        ))
        .unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", cost_fake("0.05"));
    let review = json!({
        "branch": "feature-a", "headSha": "sha-a", "target": "main",
        "task": "TKT-1", "attempt": "attempt-1",
    });
    let reviewer = spawn(&mut client, &repo_path, "review-of-TKT-1", Some(review)).await;
    wait_for_state(&mut client, &reviewer, &["completed"]).await;

    std::env::set_var("RK_FAKE_HARNESS_CMD", cost_fake("0.20"));
    let fix = spawn(&mut client, &repo_path, "TKT-2", None).await;
    wait_for_state(&mut client, &fix, &["completed"]).await;
    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-b",
            "sha-b",
            "main",
            "TKT-2",
            "landed",
            Some("merge-b"),
            at,
        ))
        .unwrap();
    space
        .out(resubmission_marker(
            "repo-a",
            "landing_rework_resubmission",
            "TKT-2",
            "TKT-1",
            at,
        ))
        .unwrap();

    // Read BEFORE the malformed generation exists: totals must be a clean,
    // deduplicated number here so the malformed case below can be isolated
    // to its own read rather than poisoning this assertion too (a single
    // malformed contribution anywhere legitimately nulls the WHOLE
    // cross-task total, by the same null-propagation rule as everything
    // else in this section — that is exercised separately below, not here).
    let before = snapshot_state(&mut client).await;
    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let after = snapshot_state(&mut client).await;
    assert_eq!(
        before, after,
        "reading native_recorded_cost through factory.scorecards must not mutate factory state"
    );

    let nrc = native_recorded_cost(&resp);
    assert_eq!(nrc["available"], json!(true));
    let tasks = nrc["tasks"].as_array().unwrap();

    let tkt1 = tasks
        .iter()
        .find(|t| t["task"] == json!("TKT-1"))
        .expect("TKT-1 present");
    assert_eq!(
        tkt1["implementation"]["settled"]["cost_usd_micro"],
        json!(100_000)
    );
    assert_eq!(tkt1["review"]["settled"]["cost_usd_micro"], json!(50_000));
    assert_eq!(
        tkt1["correction"]["settled"]["cost_usd_micro"],
        json!(200_000)
    );
    assert_eq!(tkt1["linked_correction_tickets"], json!(["TKT-2"]));
    assert_eq!(tkt1["recorded_cost_usd_micro"], json!(350_000));
    assert_eq!(tkt1["coverage_complete"], json!(true));

    let tkt2 = tasks
        .iter()
        .find(|t| t["task"] == json!("TKT-2"))
        .expect("TKT-2 present");
    assert_eq!(
        tkt2["implementation"]["settled"]["cost_usd_micro"],
        json!(200_000)
    );

    // Cross-task dedup: TKT-2's implementer contributes to both its own
    // implementation bucket and TKT-1's correction bucket — a real
    // authoritative shared contribution, named and counted once in totals.
    let shared = nrc["shared_contributions"].as_array().unwrap();
    assert_eq!(shared.len(), 1);
    let naive_sum = 100_000 + 50_000 + 200_000 + 200_000; // TKT-1's own total + TKT-2's own total
    let totals_cost = nrc["totals"]["recorded_cost_usd_micro"].as_u64().unwrap();
    assert!(
        totals_cost < naive_sum,
        "deduplicated total ({totals_cost}) must be less than the naive per-task sum ({naive_sum})"
    );

    // Now introduce the malformed (negative) harness-reported cost and read
    // again: TKT-5's own bucket is null'd, its coverage is marked incomplete,
    // and — since a single malformed contribution poisons the whole
    // cross-task dedup total by the same null-propagation rule as everything
    // else — the top-level totals go null too rather than silently reporting
    // a partial number as complete.
    std::env::set_var("RK_FAKE_HARNESS_CMD", cost_fake("-1"));
    let bad = spawn(&mut client, &repo_path, "TKT-5", None).await;
    wait_for_state(&mut client, &bad, &["completed"]).await;
    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-e",
            "sha-e",
            "main",
            "TKT-5",
            "landed",
            Some("merge-e"),
            at,
        ))
        .unwrap();

    let resp2 = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let nrc2 = native_recorded_cost(&resp2);
    let tasks2 = nrc2["tasks"].as_array().unwrap();
    let tkt5 = tasks2
        .iter()
        .find(|t| t["task"] == json!("TKT-5"))
        .expect("TKT-5 present (malformed cost)");
    assert!(tkt5["implementation"]["settled"]["cost_usd_micro"].is_null());
    assert!(tkt5["recorded_cost_usd_micro"].is_null());
    assert_eq!(tkt5["coverage_complete"], json!(false));
    assert_eq!(
        tkt5["implementation"]["settled"]["malformed_cost_generation_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        nrc2["totals"]["recorded_cost_usd_micro"].is_null(),
        "one malformed contributing generation must null the whole cross-task total, never a silent partial sum"
    );
    // TKT-1's own view is entirely unaffected by TKT-5's unrelated
    // malformed generation.
    let tkt1_again = tasks2
        .iter()
        .find(|t| t["task"] == json!("TKT-1"))
        .expect("TKT-1 still present");
    assert_eq!(tkt1_again["recorded_cost_usd_micro"], json!(350_000));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

/// The crash/respawn is real (`agent.spawn`/`agent.respawn` against the real
/// registry); the delivery attribution is seeded (`landing_processed_marker`
/// via `space.out`, not the real `LandingPipeline`) — isolating the
/// same-generation-identity join from the larger landing/review machinery,
/// same rationale as the test above. A generation that crashes, is
/// respawned, and completes on the resumed launch must be counted exactly
/// once in `native_recorded_cost` (its stable identity never changes across
/// the respawn) — not summed across attempts.
#[tokio::test]
async fn factory_analytics_native_recorded_cost_same_generation_resume_counts_once_not_summed() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, repo_dir, _layout, handle, space, mut client) = setup_with_space().await;
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    let at = fixed_clock();

    std::env::set_var("RK_FAKE_HARNESS_CMD", CRASHING_FAKE);
    let resumed = spawn(&mut client, &repo_path, "TKT-3", None).await;
    wait_for_state(&mut client, &resumed, &["failed"]).await;

    std::env::set_var("RK_FAKE_HARNESS_CMD", cost_fake("0.30"));
    client
        .call("agent.respawn", json!({"name": resumed}))
        .await
        .unwrap();
    wait_for_state(&mut client, &resumed, &["completed"]).await;

    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-c",
            "sha-c",
            "main",
            "TKT-3",
            "landed",
            Some("merge-c"),
            at,
        ))
        .unwrap();

    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let nrc = native_recorded_cost(&resp);
    let task = nrc["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["task"] == json!("TKT-3"))
        .expect("TKT-3 present");
    assert_eq!(
        task["implementation"]["settled"]["generation_count"],
        json!(1),
        "one generation identity survives the crash+respawn, not two"
    );
    assert_eq!(
        task["implementation"]["settled"]["cost_usd_micro"],
        json!(300_000),
        "resumed launch's cumulative cost, not summed across the failed attempt"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

/// The spawn and the `agent.archive` sweep are real; the delivery
/// attribution is seeded (`landing_processed_marker` via `space.out`), same
/// rationale as the two tests above. A real archive sweep moves a settled
/// generation into the archive store; `native_recorded_cost` excludes its
/// cost from the recorded sum unless the request opts into
/// `include_archived`, and flags `coverage_complete: false` while excluded
/// rather than reporting a silent partial total as complete.
#[tokio::test]
async fn factory_analytics_native_recorded_cost_archived_generation_excluded_unless_include_archived(
) {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, repo_dir, _layout, handle, space, mut client) = setup_with_space().await;
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    let at = fixed_clock();

    std::env::set_var("RK_FAKE_HARNESS_CMD", cost_fake("0.40"));
    let archived_impl = spawn(&mut client, &repo_path, "TKT-4", None).await;
    wait_for_state(&mut client, &archived_impl, &["completed"]).await;
    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-d",
            "sha-d",
            "main",
            "TKT-4",
            "landed",
            Some("merge-d"),
            at,
        ))
        .unwrap();
    client
        .call(
            "agent.archive",
            json!({"all": true, "reap_git": false, "reap_logs": false, "reap_artifacts": false}),
        )
        .await
        .unwrap();

    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let task = native_recorded_cost(&resp)["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["task"] == json!("TKT-4"))
        .expect("TKT-4 present")
        .clone();
    assert_eq!(
        task["implementation"]["settled"]["excluded_archived_generations"],
        json!(1)
    );
    assert_eq!(task["recorded_cost_usd_micro"], json!(0));
    assert_eq!(task["coverage_complete"], json!(false));

    let resp2 = client
        .call(
            "factory.scorecards",
            json!({"repo":"repo-a", "include_archived": true}),
        )
        .await
        .unwrap();
    let task2 = native_recorded_cost(&resp2)["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["task"] == json!("TKT-4"))
        .expect("TKT-4 present")
        .clone();
    assert_eq!(
        task2["implementation"]["settled"]["cost_usd_micro"],
        json!(400_000)
    );
    assert_eq!(
        task2["implementation"]["settled"]["excluded_archived_generations"],
        json!(0)
    );
    assert_eq!(task2["coverage_complete"], json!(true));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

/// No real agent spawns here — pure seeded markers over the real bounded
/// storage query, isolating an edge case (ambiguous cross-task linkage) the
/// real pipeline would rarely produce on its own. Two markers linking the
/// SAME rework ticket to two different original tasks (ambiguous, excluded
/// from every task's correction cost rather than guessed), and a marker
/// written under a different repo scope (must never leak into `repo-a`'s
/// coverage).
#[tokio::test]
async fn factory_analytics_native_recorded_cost_ambiguous_link_and_cross_repo_scoping_via_real_markers(
) {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, space, mut client) = setup_with_space().await;
    let at = fixed_clock();

    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-1",
            "sha-1",
            "main",
            "TKT-1",
            "landed",
            Some("merge-1"),
            at,
        ))
        .unwrap();
    space
        .out(landing_processed_marker(
            "repo-a",
            "feature-4",
            "sha-4",
            "main",
            "TKT-4",
            "landed",
            Some("merge-4"),
            at,
        ))
        .unwrap();
    space
        .out(resubmission_marker(
            "repo-a",
            "landing_rework_resubmission",
            "TKT-9",
            "TKT-1",
            at,
        ))
        .unwrap();
    space
        .out(resubmission_marker(
            "repo-a",
            "landing_rework_resubmission",
            "TKT-9",
            "TKT-4",
            at,
        ))
        .unwrap();
    // A different repo's own resubmission marker must never leak in.
    space
        .out(landing_processed_marker(
            "repo-other",
            "feature-x",
            "sha-x",
            "main",
            "TKT-X",
            "landed",
            Some("merge-x"),
            at,
        ))
        .unwrap();
    space
        .out(resubmission_marker(
            "repo-other",
            "landing_rework_resubmission",
            "TKT-X2",
            "TKT-X",
            at,
        ))
        .unwrap();

    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let nrc = native_recorded_cost(&resp);
    assert!(nrc["ambiguous_correction_tickets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "TKT-9"));
    let tasks = nrc["tasks"].as_array().unwrap();
    assert!(
        tasks.iter().all(|t| t["task"] != json!("TKT-X")),
        "a different repo's task must never appear under repo-a's coverage"
    );
    for task in tasks {
        assert_eq!(
            task["correction"]["settled"]["generation_count"],
            json!(0),
            "the ambiguous ticket must not be attributed to either candidate task"
        );
    }

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

/// The two resubmission-marker identities (`landing_rework_resubmission` /
/// `landing_conflict_rework_resubmission`) genuinely share ONE
/// `MAX_SCAN_TUPLES` bounded-read budget rather than each getting a full cap
/// of its own — proven against the real storage query, matching
/// `factory_analytics_native_delivery_truncates_at_the_real_storage_cap`'s
/// pattern for `native_delivery`.
#[tokio::test]
async fn factory_analytics_native_recorded_cost_correction_links_share_one_bounded_budget() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let (_home, _repo, _layout, handle, space, mut client) = setup_with_space().await;
    let at = fixed_clock();
    const MAX_SCAN_TUPLES: usize = 10_000;
    // Split across BOTH identities so a per-identity cap (a regression back
    // to 2x the budget) would NOT show truncation here, while a genuinely
    // shared cap does: 6_000 + 4_001 = one over MAX_SCAN_TUPLES.
    for i in 0..6_000 {
        space
            .out(resubmission_marker(
                "repo-a",
                "landing_rework_resubmission",
                &format!("TKT-R{i}"),
                &format!("TKT-ORIG-{i}"),
                at,
            ))
            .unwrap();
    }
    for i in 0..=4_000 {
        space
            .out(resubmission_marker(
                "repo-a",
                "landing_conflict_rework_resubmission",
                &format!("TKT-C{i}"),
                &format!("TKT-ORIG2-{i}"),
                at,
            ))
            .unwrap();
    }

    let resp = client
        .call("factory.scorecards", json!({"repo":"repo-a"}))
        .await
        .unwrap();
    let nrc = native_recorded_cost(&resp);
    assert_eq!(
        nrc["coverage"]["correction_links"]["limit"],
        json!(MAX_SCAN_TUPLES)
    );
    assert_eq!(
        nrc["coverage"]["correction_links"]["scanned"],
        json!(MAX_SCAN_TUPLES),
        "shared budget across both identities, not 2x MAX_SCAN_TUPLES"
    );
    assert_eq!(
        nrc["coverage"]["correction_links"]["truncated"],
        json!(true)
    );
    assert_eq!(nrc["coverage"]["may_hide_contributors"], json!(true));

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
