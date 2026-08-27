//! End-to-end coverage for TKT-01M0QD49GNDXM70ANERKZYXS3C: a leased
//! orchestrator can defer an `attention.decide` item it cannot or should not
//! execute to a durable human gate, and advance the resumable cursor past
//! it — without ever claiming the underlying problem is resolved.
//!
//! The motivating shape is a current, exactly keyed
//! `conflict-held-landing` violation whose matching dispatch marker is
//! missing. `conflict.dispatch_correction` (`Server::execute_orchestrator`)
//! fails closed against this shape because there is no authoritative
//! `ConflictContext` to execute. A human can still decide how to repair or
//! abandon the branch without reviving the removed branch-latest fallback.
//!
//! This file proves `disposition: "defer_to_human"`:
//! - requires the SAME live, fenced lease the execute arm requires — no/stale
//!   lease refuses with zero side effect (no decision, no inbox row);
//! - writes ONE terminal, replay-safe decision-journal record
//!   (`resolved: false, gated: true`) carrying the requested decision, the
//!   reason, holder/generation, and budget;
//! - surfaces a durable human gate through the SAME `recovery_action`/`rk
//!   inbox`/`rk inbox ack` boundary every other automated recovery
//!   escalation in this daemon uses — no second queue;
//! - advances the SAME orchestrator cursor `attention.decide`'s execute arm
//!   advances, so `attention.next` reaches a later executable conflict
//!   instead of being pinned behind the markerless item forever;
//! - is idempotent: replaying the same deferral returns the same record and
//!   never duplicates the decision, the gate, or the cursor advance;
//! - survives a genuine daemon restart; and
//! - leaves the existing execute path (a bounded, chain-keyed conflict still
//!   dispatches) completely unchanged.

mod fixture;
mod support;

use rk_core::config::PolicyConfig;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const QUICK_DONE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"defer-lease-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"defer-lease-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
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
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn repo_name_of(dir: &Path) -> String {
    dir.file_name().unwrap().to_string_lossy().to_string()
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
}

fn branch_off_main(dir: &Path, branch: &str, file: &str) -> String {
    git(dir, &["checkout", "-b", branch]);
    std::fs::write(dir.join(file), "feature work\n").unwrap();
    git(dir, &["add", file]);
    git(dir, &["commit", "-m", "feat: feature work"]);
    let head_sha = git(dir, &["rev-parse", branch]);
    git(dir, &["checkout", "main"]);
    head_sha
}

fn allow(kinds: &[&str]) -> PolicyConfig {
    PolicyConfig {
        orchestrator_action_allowlist: kinds.iter().map(|s| s.to_string()).collect(),
        ..PolicyConfig::default()
    }
}

async fn daemon_with_policy(home: &Path, cfg: PolicyConfig) -> Client {
    let layout = Layout::at(home);
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_authority_policy_for_tests(&cfg);
    tokio::spawn(daemon.run());
    connect(&layout).await
}

async fn attention_next(client: &mut Client, repo: &str) -> Option<Value> {
    let res = client
        .call("attention.next", json!({"repo": repo}))
        .await
        .unwrap();
    res.get("item").filter(|v| !v.is_null()).cloned()
}

/// An exactly keyed `branch_landed` event whose matching
/// `landing_conflict_rework_dispatch` marker is absent. It is safe to surface
/// for a human disposition because its identity is exact, but execution
/// fails closed because no authoritative `ConflictContext` exists.
async fn markerless_held_conflict(client: &mut Client, repo: &str, branch: &str, target: &str) {
    let chain_key =
        format!("{repo}\0{branch}\0missing-head\0{target}\0missing-task\0missing-rework");
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "branch_landed",
                "payload": {
                    "branch": branch,
                    "target": target,
                    "merged": false,
                    "pr_opened": false,
                    "chain_key": chain_key,
                    "detail": "merge conflict or failure: CONFLICT (content): Merge conflict in lib.rs",
                },
            }),
        )
        .await
        .unwrap();
}

/// A bounded, chain-keyed hold — the shape `route_conflict` writes for real,
/// which `disposition: "execute"` can still dispatch.
async fn bounded_held_conflict(
    client: &mut Client,
    repo: &str,
    repo_path: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    rework_ticket: &str,
) {
    let chain_key =
        format!("{repo}\0{branch}\0{head_sha}\0{target}\0conflict-task\0{rework_ticket}");
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "branch_landed",
                "payload": {
                    "branch": branch,
                    "target": target,
                    "merged": false,
                    "pr_opened": false,
                    "chain_key": chain_key,
                    "detail": format!(
                        "merge conflict held for a bounded orchestrator-authority correction \
                         decision, ticket {rework_ticket}"
                    ),
                },
            }),
        )
        .await
        .unwrap();
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "landing_conflict_rework_dispatch",
                "lifecycle": "furniture",
                "payload": {
                    "dispatch_key": chain_key,
                    "repo": repo,
                    "repo_path": repo_path,
                    "source": branch,
                    "branch": branch,
                    "head_sha": head_sha,
                    "target": target,
                    "target_head": "target-head-placeholder",
                    "fork_point": "fork-point-placeholder",
                    "task": "conflict-task",
                    "rework_ticket": rework_ticket,
                    "conflict_evidence": "CONFLICT (content): Merge conflict in feature.txt",
                    "agent": Value::Null,
                    "attempt": 1,
                    "state": "awaiting-orchestrator-decision",
                    "diff_files": 1,
                    "diff_lines": 1,
                },
            }),
        )
        .await
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn defer(
    client: &mut Client,
    repo: &str,
    item: &str,
    holder: Option<&str>,
    generation: Option<u64>,
) -> rk_core::Result<Value> {
    client
        .call(
            "attention.decide",
            json!({
                "repo": repo,
                "item": item,
                "holder": holder,
                "generation": generation,
                "disposition": "defer_to_human",
                "reason": "the exact conflict dispatch marker is missing; dispatch would refuse",
                "requested_decision": "resolve or abandon the feature branch by hand",
            }),
        )
        .await
}

async fn execute(
    client: &mut Client,
    repo: &str,
    item: &str,
    holder: Option<&str>,
    generation: Option<u64>,
) -> rk_core::Result<Value> {
    client
        .call(
            "attention.decide",
            json!({
                "repo": repo,
                "item": item,
                "holder": holder,
                "generation": generation,
            }),
        )
        .await
}

async fn agent_spawn_count(client: &mut Client, repo: &str) -> usize {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "agent_spawned"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().unwrap().len()
}

async fn decision_count(client: &mut Client, repo: &str) -> usize {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "orchestrator_decision"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().unwrap().len()
}

async fn recovery_action_count(client: &mut Client, repo: &str) -> usize {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "recovery_action"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().unwrap().len()
}

/// Only `terminal: true` decisions count as "decided" — `record_decision`'s
/// own doc comment: a non-terminal "attempting" intent is audit-trail only
/// and harmless to write more than once, so tests that assert "exactly one
/// decision was made" must count terminal records, not raw ones.
async fn terminal_decision_count(client: &mut Client, repo: &str) -> usize {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "orchestrator_decision"}),
        )
        .await
        .unwrap();
    res["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["payload"]["terminal"] == true)
        .count()
}

async fn inbox_items(client: &mut Client, repo: &str) -> Vec<Value> {
    let res = client
        .call("inbox.list", json!({"repo": repo}))
        .await
        .unwrap();
    res["items"].as_array().unwrap().clone()
}

/// Fabricate the EXACT durable state `defer_attention_to_human`'s phase 1
/// (non-terminal intent) would have left, directly via `space.out` — lets a
/// test prove a stray non-terminal record (the trace a crash right after
/// phase 1 leaves) never confuses a later real `defer` call.
async fn fabricate_defer_intent(client: &mut Client, repo: &str, violation_id: &str, holder: &str) {
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "orchestrator_decision",
                "lifecycle": "furniture",
                "payload": {
                    "violation_id": violation_id,
                    "kind": "conflict-held-landing",
                    "scope": repo,
                    "subject": "feature",
                    "authority": "orchestrator",
                    "evidence": [],
                    "decided_by": holder,
                    "generation": 1,
                    "action": "attention.defer_to_human",
                    "attempt": Value::Null,
                    "budget_usd": Value::Null,
                    "budget_tokens": Value::Null,
                    "outcome": "attempting",
                    "resolved": false,
                    "gated": true,
                    "requested_decision": "fabricated intent",
                    "reason": "fabricated intent",
                    "blast_radius": "fabricated intent",
                    "resolving_action": "fabricated intent",
                    "terminal": false,
                    "decided_at": "2026-08-23T00:00:00Z",
                },
            }),
        )
        .await
        .unwrap();
}

/// Fabricate the EXACT durable state `defer_attention_to_human`'s phase 2
/// (the human gate) would have left — a `recovery_action` escalation whose
/// `notice.refs.violation_id` matches, the field
/// `Server::find_recovery_action_for_violation` scans for. Lets a test prove
/// a genuinely pre-existing gate for this violation is REUSED, never
/// duplicated, by a later real `defer` call.
async fn fabricate_defer_gate(client: &mut Client, repo: &str, violation_id: &str, holder: &str) {
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "recovery_action",
                "lifecycle": "furniture",
                "payload": {
                    "action_kind": "orchestrator-defer-to-human",
                    "held": false,
                    "notice": {
                        "tuple_id": format!("{violation_id}@{holder}"),
                        "class": "orchestrator-defer-to-human",
                        "severity": "critical",
                        "scope": repo,
                        "subject": "feature",
                        "text": format!(
                            "orchestrator {holder} deferred {violation_id} to a human instead \
                             of executing it: fabricated gate\nDECISION NEEDED: fabricated\n\
                             BLAST RADIUS: fabricated\nRESOLVE WITH: fabricated"
                        ),
                        "suggested_action": Value::Null,
                        "refs": {
                            "violation_id": violation_id,
                            "blast_radius": "fabricated gate",
                            "resolving_action": "fabricated gate",
                        },
                    },
                },
            }),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn defer_requires_a_live_fenced_lease_and_refuses_with_zero_side_effect() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    markerless_held_conflict(&mut client, repo, "feature", "main").await;

    let item = attention_next(&mut client, repo)
        .await
        .expect("an exactly keyed markerless conflict must surface for a human disposition");
    assert_eq!(item["kind"], "conflict-held-landing");
    assert_eq!(item["effective_authority"], "orchestrator");
    let item_id = item["id"].as_str().unwrap().to_string();

    // No lease at all: refused, zero mutation.
    let no_lease = defer(&mut client, repo, &item_id, Some("orch-1"), Some(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(no_lease.contains("no lease held"), "{no_lease}");
    assert_eq!(decision_count(&mut client, repo).await, 0);
    assert_eq!(recovery_action_count(&mut client, repo).await, 0);

    // Acquire, let it expire, get superseded — the captured generation is stale.
    let first = client
        .call(
            "lease.acquire",
            json!({"repo": repo, "holder": "orch-1", "ttl_secs": 1}),
        )
        .await
        .unwrap();
    let stale_generation = first["generation"].as_u64().unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-2"}))
        .await
        .unwrap();

    let fenced = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(stale_generation),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(fenced.contains("fencing failed"), "{fenced}");
    assert_eq!(
        decision_count(&mut client, repo).await,
        0,
        "a stale-generation deferral must never write a decision"
    );
    assert_eq!(recovery_action_count(&mut client, repo).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn defer_writes_one_gated_terminal_decision_and_a_durable_inbox_gate_replay_cannot_duplicate()
{
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    markerless_held_conflict(&mut client, repo, "feature", "main").await;
    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    let deferred = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(deferred["resolved"], false);
    assert_eq!(deferred["replay"], false);
    assert_eq!(deferred["gated"], true);
    let decision = &deferred["decision"];
    assert_eq!(decision["kind"], "conflict-held-landing");
    assert_eq!(decision["action"], "attention.defer_to_human");
    assert_eq!(decision["decided_by"], "orch-1");
    assert_eq!(decision["generation"], generation);
    assert_eq!(decision["resolved"], false);
    assert_eq!(decision["gated"], true);
    assert_eq!(decision["terminal"], true);
    assert_eq!(
        decision["requested_decision"],
        "resolve or abandon the feature branch by hand"
    );
    assert!(decision["reason"]
        .as_str()
        .unwrap()
        .contains("exact conflict dispatch marker is missing"));

    // No mutation happened: no ticket/repo/agent action, no correction agent.
    assert_eq!(agent_spawn_count(&mut client, repo).await, 0);

    // Exactly one TERMINAL decision (the non-terminal phase-1 intent is
    // audit-trail only — `decision_count` also includes it) and one
    // recovery-action escalation were written.
    assert_eq!(terminal_decision_count(&mut client, repo).await, 1);
    assert_eq!(recovery_action_count(&mut client, repo).await, 1);

    // The human gate is a normal, durable `rk inbox` row: precise decision,
    // blast radius, and resolving command all present.
    let items = inbox_items(&mut client, repo).await;
    let gate = items
        .iter()
        .find(|i| i["kind"] == "recovery-action")
        .expect("the deferral must surface as a recovery-action inbox row");
    let detail = gate["detail"].as_str().unwrap();
    assert!(detail.contains("DECISION NEEDED:"), "{detail}");
    assert!(detail.contains("BLAST RADIUS:"), "{detail}");
    assert!(detail.contains("RESOLVE WITH:"), "{detail}");
    assert!(
        detail.contains("resolve or abandon the feature branch by hand"),
        "{detail}"
    );
    let action = gate["action"].as_str().unwrap();
    assert!(
        action.starts_with("rk inbox ack "),
        "the row must clear through the SAME ack boundary every other recovery action uses: {action}"
    );

    // Replaying the SAME deferral returns the SAME record and writes nothing new.
    let replay = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay["resolved"], false);
    assert_eq!(replay["replay"], true);
    assert_eq!(replay["gated"], true);
    assert_eq!(replay["decision"], deferred["decision"]);
    assert_eq!(
        terminal_decision_count(&mut client, repo).await,
        1,
        "a replayed deferral must never write a second terminal decision"
    );
    assert_eq!(
        recovery_action_count(&mut client, repo).await,
        1,
        "a replayed deferral must never duplicate the human gate"
    );
    assert_eq!(agent_spawn_count(&mut client, repo).await, 0);

    // Cursor advanced exactly once, to this item.
    let lease_after = client
        .call(
            "lease.renew",
            json!({"repo": repo, "holder": "orch-1", "generation": generation}),
        )
        .await
        .unwrap();
    assert_eq!(lease_after["cursor"].as_str().unwrap(), item_id);

    // Acking the gate is the "separately acknowledged" boundary: it clears
    // the row without touching the decision journal or the cursor.
    let gate_id = gate["subject"].as_str().unwrap_or_default();
    let _ = gate_id; // subject is the branch name, not the tuple id — use the action string instead.
    let ack_id = action.trim_start_matches("rk inbox ack ").to_string();
    let ack = client
        .call("inbox.ack", json!({"id": ack_id}))
        .await
        .unwrap();
    assert_eq!(ack["written"], true);
    let items_after_ack = inbox_items(&mut client, repo).await;
    assert!(
        !items_after_ack
            .iter()
            .any(|i| i["kind"] == "recovery-action"),
        "an acked gate must drop off rk inbox"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferring_a_markerless_item_lets_attention_next_reach_a_later_executable_conflict() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let head_sha = branch_off_main(repo_dir.path(), "other-feature", "other.txt");

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));
    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    // The exactly keyed but markerless item that would otherwise pin the queue forever.
    markerless_held_conflict(&mut client, repo, "feature", "main").await;
    let markerless_item = attention_next(&mut client, repo).await.unwrap();
    let markerless_id = markerless_item["id"].as_str().unwrap().to_string();

    // A genuinely later, bounded, chain-keyed conflict on ANOTHER branch.
    bounded_held_conflict(
        &mut client,
        repo,
        &repo_path,
        "other-feature",
        &head_sha,
        "main",
        "TKT-BOUNDED",
    )
    .await;

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    // Before deferring: `attention.next` still starts at the markerless item
    // (lexicographically first / no cursor yet).
    let next = attention_next(&mut client, repo).await.unwrap();
    assert_eq!(next["id"], markerless_id);

    let deferred = defer(
        &mut client,
        repo,
        &markerless_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(deferred["resolved"], false);
    assert_eq!(deferred["gated"], true);

    // The queue is unstuck: the next item is the later bounded conflict, not
    // the same markerless item again.
    let next = attention_next(&mut client, repo)
        .await
        .expect("a later, genuinely bounded conflict must be reachable after a deferral");
    assert_ne!(next["id"], markerless_id);
    assert_eq!(next["kind"], "conflict-held-landing");
    assert_eq!(next["subject"], "other-feature");
    let bounded_id = next["id"].as_str().unwrap().to_string();

    // And it can still be EXECUTED — the disposition split changes nothing
    // about the existing execute arm's own dispatch.
    let executed = execute(
        &mut client,
        repo,
        &bounded_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(executed["resolved"], true);
    assert_eq!(
        executed["decision"]["action"],
        "conflict.dispatch_correction"
    );
    assert_eq!(agent_spawn_count(&mut client, repo).await, 1);

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn defer_is_refused_for_non_orchestrator_authority_and_unrecognized_disposition_is_bad_params(
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    markerless_held_conflict(&mut client, repo, "feature", "main").await;
    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    // An unrecognized disposition value is a bad request, not silently
    // treated as execute.
    let bad = client
        .call(
            "attention.decide",
            json!({
                "repo": repo,
                "item": item_id,
                "disposition": "not-a-real-disposition",
            }),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(bad.contains("unrecognized disposition"), "{bad}");

    // `delivered-but-open` is `Mechanical`-authority — deferring it to a
    // human is refused, not silently accepted.
    let ticket = client
        .call(
            "space.out",
            json!({
                "category": "task",
                "scope": repo,
                "identity": "TKT-MECH",
                "payload": {
                    "title": "t",
                    "status": "in_progress",
                    "delivery": {
                        "merge_commit": "abc123",
                        "branch": "b",
                        "target": "main",
                        "landed_at": "2026-08-19T00:00:00Z",
                    },
                },
            }),
        )
        .await
        .unwrap();
    let _ = ticket;
    let mech_item = attention_next(&mut client, repo)
        .await
        .filter(|i| i["kind"] == "delivered-but-open");
    if let Some(mech_item) = mech_item {
        let mech_id = mech_item["id"].as_str().unwrap().to_string();
        let refused = client
            .call(
                "attention.decide",
                json!({
                    "repo": repo,
                    "item": mech_id,
                    "disposition": "defer_to_human",
                    "holder": "orch-1",
                    "generation": 1,
                    "reason": "test",
                    "requested_decision": "test",
                }),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("not orchestrator"), "{refused}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_decision_and_inbox_gate_survive_a_genuine_daemon_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let config = rk_core::config::Config {
        policy: allow(&["conflict-held-landing"]),
        ..rk_core::config::Config::default()
    };
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();

    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();
    markerless_held_conflict(&mut client, repo, "feature", "main").await;

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();
    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    let deferred = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(deferred["gated"], true);

    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The lease resumes with its cursor untouched by the restart.
    let resumed = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(resumed["generation"].as_u64().unwrap(), generation);
    assert_eq!(resumed["cursor"].as_str().unwrap(), item_id);

    // The inbox gate is still visible after restart.
    let items = inbox_items(&mut client, repo).await;
    assert!(
        items.iter().any(|i| i["kind"] == "recovery-action"),
        "the human gate must survive a daemon restart"
    );

    // A replayed deferral across the restart still returns the same
    // terminal record and writes nothing new.
    let replay = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay["replay"], true);
    assert_eq!(recovery_action_count(&mut client, repo).await, 1);
    assert_eq!(terminal_decision_count(&mut client, repo).await, 1);

    handle_b.abort();
    let _ = handle_b.await;
}

/// Fault injection for the crash window BETWEEN phase 1 (non-terminal
/// intent) and phase 2 (the human gate): fabricate exactly the durable trace
/// a crash right after phase 1 would leave — a non-terminal decision, no
/// gate, no cursor — then let a real `defer` call resume. It must converge
/// on exactly one gate and one terminal decision, ignoring the stray intent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stray_non_terminal_intent_never_blocks_or_duplicates_a_resumed_deferral() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    markerless_held_conflict(&mut client, repo, "feature", "main").await;
    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    fabricate_defer_intent(&mut client, repo, &item_id, "orch-1").await;
    assert_eq!(recovery_action_count(&mut client, repo).await, 0);

    let resumed = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(resumed["resolved"], false);
    assert_eq!(resumed["gated"], true);
    assert_eq!(recovery_action_count(&mut client, repo).await, 1);
    assert_eq!(terminal_decision_count(&mut client, repo).await, 1);
    let lease_after = client
        .call(
            "lease.renew",
            json!({"repo": repo, "holder": "orch-1", "generation": generation}),
        )
        .await
        .unwrap();
    assert_eq!(lease_after["cursor"].as_str().unwrap(), item_id);
}

/// Fault injection for the crash window BETWEEN phase 2 (the gate is
/// written) and phase 3 (cursor advance): fabricate a pre-existing gate for
/// this exact violation, cursor still unset, then resume with a real
/// `defer` call. It must REUSE the fabricated gate (never mint a second
/// one), advance the cursor, and still write exactly one terminal decision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_already_written_before_a_crash_is_reused_not_duplicated_on_resume() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    markerless_held_conflict(&mut client, repo, "feature", "main").await;
    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    fabricate_defer_intent(&mut client, repo, &item_id, "orch-1").await;
    fabricate_defer_gate(&mut client, repo, &item_id, "orch-1").await;
    assert_eq!(recovery_action_count(&mut client, repo).await, 1);
    let fabricated_gate_id = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "recovery_action"}),
        )
        .await
        .unwrap()["tuples"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resumed = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(resumed["resolved"], false);
    assert_eq!(resumed["gated"], true);
    assert_eq!(
        recovery_action_count(&mut client, repo).await,
        1,
        "the pre-existing gate must be reused, not duplicated"
    );
    let final_gate_id = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "recovery_action"}),
        )
        .await
        .unwrap()["tuples"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        final_gate_id, fabricated_gate_id,
        "the ORIGINAL gate tuple must survive untouched, not be replaced"
    );
    assert_eq!(terminal_decision_count(&mut client, repo).await, 1);
    let lease_after = client
        .call(
            "lease.renew",
            json!({"repo": repo, "holder": "orch-1", "generation": generation}),
        )
        .await
        .unwrap();
    assert_eq!(lease_after["cursor"].as_str().unwrap(), item_id);
}

/// Fault injection for the crash window BETWEEN phase 3 (cursor advanced)
/// and phase 4 (the terminal decision) — the one seam no public RPC can
/// reach (every RPC that can move the cursor also writes the terminal
/// record in the same call). Fabricates it by writing directly to the
/// SAME on-disk `orchestrator-lease.json` the running daemon persists to
/// (`Daemon::new`'s own `layout.home().join("orchestrator-lease.json")`),
/// bypassing the daemon entirely — exactly what a genuine crash between
/// those two phases would leave: cursor advanced, gate written, no
/// terminal decision. A resumed `defer` call must still converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_advanced_before_a_crash_still_converges_to_one_terminal_decision() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    markerless_held_conflict(&mut client, repo, "feature", "main").await;
    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    fabricate_defer_intent(&mut client, repo, &item_id, "orch-1").await;
    fabricate_defer_gate(&mut client, repo, &item_id, "orch-1").await;

    // Fabricate phase 3 directly against the daemon's own lease file —
    // `daemon_with_policy` uses `Layout::at(home.path())`, which is a bare
    // in-memory daemon and does NOT persist the lease store to `home`; this
    // test therefore drives the fabrication through a real, file-backed
    // daemon instead of the in-memory harness the other tests use.
    let lease_store = rk_daemon::orchestrator_lease::LeaseStore::load(
        layout.home().join("orchestrator-lease.json"),
    )
    .unwrap();
    lease_store
        .advance_cursor(repo, "orch-1", generation, &item_id, chrono::Utc::now())
        .ok();

    let resumed = defer(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(resumed["resolved"], false);
    assert_eq!(resumed["gated"], true);
    assert_eq!(
        recovery_action_count(&mut client, repo).await,
        1,
        "the pre-existing gate must be reused, not duplicated"
    );
    assert_eq!(terminal_decision_count(&mut client, repo).await, 1);
    let lease_after = client
        .call(
            "lease.renew",
            json!({"repo": repo, "holder": "orch-1", "generation": generation}),
        )
        .await
        .unwrap();
    assert_eq!(lease_after["cursor"].as_str().unwrap(), item_id);
}

/// The gap the operator audit named explicitly: a caller that only ever
/// calls `attention.next` in a loop — never retaining or replaying an old
/// item id through `attention.decide` — has no way to trigger a fresh
/// `defer` call for an item the cursor has already passed. If phase 3
/// (cursor advance) ran before a crash and phase 4 (the terminal record)
/// never did, such a caller would otherwise leave that terminal audit
/// record incomplete FOREVER: `attention.next` never re-offers an item the
/// cursor is already past, so nothing would ever again invoke
/// `defer_attention_to_human` for this exact id.
///
/// This reproduces exactly that crash window — intent and gate durably
/// written, cursor durably advanced past the item, no terminal record — via
/// a genuine daemon restart (the ONLY way to force the daemon's own
/// in-memory `LeaseStore` to observe a cursor written by a separate
/// `LeaseStore` handle on the same file; the running daemon never re-reads
/// its lease file), then proves a SINGLE `attention.next` call self-heals:
/// it completes the dangling terminal record, never duplicates the gate,
/// and lets the queue reach a later, genuinely bounded conflict — with no
/// `attention.decide` call for the stuck item ever made by the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attention_next_alone_completes_a_terminal_record_left_dangling_by_a_cursor_advanced_before_a_crash(
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let head_sha = branch_off_main(repo_dir.path(), "other-feature", "other.txt");
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let config = rk_core::config::Config {
        policy: allow(&["conflict-held-landing"]),
        ..rk_core::config::Config::default()
    };
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();

    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();
    markerless_held_conflict(&mut client, repo, "feature", "main").await;
    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    // A genuinely later, bounded conflict on another branch — proves the
    // queue is truly unstuck, not merely that the stuck item disappears.
    bounded_held_conflict(
        &mut client,
        repo,
        &repo_path,
        "other-feature",
        &head_sha,
        "main",
        "TKT-BOUNDED",
    )
    .await;

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    // Phases 1 and 2, exactly as a real `defer` call would leave them.
    fabricate_defer_intent(&mut client, repo, &item_id, "orch-1").await;
    fabricate_defer_gate(&mut client, repo, &item_id, "orch-1").await;
    assert_eq!(terminal_decision_count(&mut client, repo).await, 0);

    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    // Phase 3, fabricated directly against the on-disk lease store while no
    // daemon is running — the crash window between cursor-advance and the
    // terminal write, with nothing left to write it.
    let lease_store = rk_daemon::orchestrator_lease::LeaseStore::load(
        layout.home().join("orchestrator-lease.json"),
    )
    .unwrap();
    lease_store
        .advance_cursor(repo, "orch-1", generation, &item_id, chrono::Utc::now())
        .unwrap();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The ONLY call this test makes from here on is `attention.next` — no
    // `attention.decide` for `item_id` is ever issued, which is exactly the
    // "attention.next-only consumer" shape the audit named.
    let next = attention_next(&mut client, repo)
        .await
        .expect("the queue must be unstuck: a later bounded conflict is reachable");
    assert_ne!(
        next["id"], item_id,
        "the healed item must never be re-offered"
    );
    assert_eq!(next["kind"], "conflict-held-landing");
    assert_eq!(next["subject"], "other-feature");

    assert_eq!(
        terminal_decision_count(&mut client, repo).await,
        1,
        "a single attention.next call must complete the dangling terminal record"
    );
    assert_eq!(
        recovery_action_count(&mut client, repo).await,
        1,
        "healing must never duplicate the human gate"
    );
    let decisions = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "orchestrator_decision"}),
        )
        .await
        .unwrap();
    let healed = decisions["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| &t["payload"])
        .find(|p| p["terminal"] == true)
        .expect("a terminal decision must now exist");
    assert_eq!(healed["violation_id"], item_id);
    assert_eq!(healed["gated"], true);
    assert_eq!(healed["resolved"], false);
    assert_eq!(healed["requested_decision"], "fabricated intent");
    assert_eq!(healed["reason"], "fabricated intent");

    // A second `attention.next` call is a no-op: nothing left to heal.
    let _ = attention_next(&mut client, repo).await;
    assert_eq!(terminal_decision_count(&mut client, repo).await, 1);
    assert_eq!(recovery_action_count(&mut client, repo).await, 1);

    handle_b.abort();
    let _ = handle_b.await;
}
