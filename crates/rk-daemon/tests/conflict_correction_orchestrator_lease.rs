//! End-to-end coverage for TKT-01M0Q5EJW8NYQEYNM6QKN0H7R0: a bounded
//! merge-CONFLICT correction is `Authority::Orchestrator` (`landing_conflict.rs`),
//! but that authority alone must never spawn a correction agent unattended.
//! `landing::LandingPipeline::route_conflict` now only collects evidence and
//! holds — `crate::landing_conflict::CONFLICT_STATE_AWAITING_DECISION` — and
//! the actual `spawn_async` mutation
//! (`LandingPipeline::dispatch_held_conflict`) is reachable ONLY through
//! `Server::execute_orchestrator`, itself only reachable after
//! `attention.decide`'s `Authority::Orchestrator` arm has fenced the call
//! through a live `crate::orchestrator_lease::LeaseStore` lease — the SAME
//! lease/decision-journal boundary `authority_ladder.rs` already proves for
//! `terminal-assignee-active-work`, exercised here end-to-end for
//! `conflict-held-landing` instead.
//!
//! Rather than driving a real unmergeable branch through the whole landing
//! queue (`prepare_merge` conflict detection is already unit-tested in
//! `landing.rs`), this file fabricates the exact durable evidence
//! `route_conflict`'s hold path writes — a `branch_landed` event and a
//! `landing_conflict_rework_dispatch` marker in state
//! `awaiting-orchestrator-decision` — against a REAL git repo and branch, so
//! `dispatch_held_conflict`'s own tip check and `agent.spawn` call are
//! exercised for real, not mocked.
//!
//! Acceptance-criteria proofs:
//! - no-lease refusal, stale-generation fencing, valid-lease dispatch
//!   (a real agent spawns), duplicate-decision replay, and lease/journal
//!   survival across a genuine daemon restart all live here.
//! - human gates (protected path, oversized diff) and bounded-chain
//!   exhaustion are UNCHANGED by this ticket — `route_conflict`'s
//!   `ReworkRoute::Withhold` arm never reaches the orchestrator hold at all
//!   — and stay covered by `landing.rs`'s own
//!   `protected_path_conflict_holds_for_a_human_instead_of_dispatching` and
//!   `conflict_recovery_chain_exhausted_holds_once_with_actionable_evidence`,
//!   not duplicated here.

mod fixture;
mod support;

use rk_core::config::{Config, PolicyConfig};
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// Process-global, like `authority_ladder.rs`'s identical guard: every test
/// in this file that spawns an agent uses the same fake-harness script.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const QUICK_DONE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"conflict-lease-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"conflict-lease-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
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
}

/// A `feature` branch with one commit of its own, diverged from `main` —
/// realistic enough for `dispatch_held_conflict`'s own tip check, without
/// needing an actually-unmergeable diff (`prepare_merge`'s conflict
/// detection is exercised elsewhere, in `landing.rs`'s own unit tests).
fn branch_off_main(dir: &Path) -> String {
    git(dir, &["checkout", "-b", "feature"]);
    std::fs::write(dir.join("feature.txt"), "feature work\n").unwrap();
    git(dir, &["add", "feature.txt"]);
    git(dir, &["commit", "-m", "feat: feature work"]);
    let head_sha = git(dir, &["rev-parse", "feature"]);
    git(dir, &["checkout", "main"]);
    head_sha
}

/// A second, genuinely distinct commit on an existing `branch` — simulates a
/// held conflict's correction landing back on the branch, then conflicting
/// again with `target` a second time. Returns the new tip.
fn advance_branch(dir: &Path, branch: &str, file: &str) -> String {
    git(dir, &["checkout", branch]);
    std::fs::write(dir.join(file), "more feature work\n").unwrap();
    git(dir, &["add", file]);
    git(dir, &["commit", "-m", &format!("feat: {file}")]);
    let head_sha = git(dir, &["rev-parse", branch]);
    git(dir, &["checkout", "main"]);
    head_sha
}

async fn attention_next(client: &mut Client, repo: &str) -> Option<Value> {
    let res = client
        .call("attention.next", json!({"repo": repo}))
        .await
        .unwrap();
    res.get("item").filter(|v| !v.is_null()).cloned()
}

async fn decide(
    client: &mut Client,
    repo: &str,
    item: &str,
    holder: Option<&str>,
    generation: Option<u64>,
) -> rk_core::Result<Value> {
    decide_with_budget(client, repo, item, holder, generation, None, None).await
}

#[allow(clippy::too_many_arguments)]
async fn decide_with_budget(
    client: &mut Client,
    repo: &str,
    item: &str,
    holder: Option<&str>,
    generation: Option<u64>,
    budget_usd: Option<f64>,
    budget_tokens: Option<u64>,
) -> rk_core::Result<Value> {
    client
        .call(
            "attention.decide",
            json!({
                "repo": repo,
                "item": item,
                "holder": holder,
                "generation": generation,
                "budget_usd": budget_usd,
                "budget_tokens": budget_tokens,
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

/// The `task` (rework ticket) of every `agent_spawned` event recorded so
/// far — lets a test prove WHICH chain's ticket a dispatch actually spawned
/// for, not just that some dispatch happened.
async fn agent_spawned_tasks(client: &mut Client, repo: &str) -> Vec<String> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "agent_spawned"}),
        )
        .await
        .unwrap();
    res["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["payload"]["task"].as_str().unwrap().to_string())
        .collect()
}

/// Every `landing_conflict_rework_dispatch` marker payload written for the
/// exact `dispatch_key` a chain's `hold_conflict` used — lets a test prove a
/// chain's OWN markers were untouched (still just its original
/// `awaiting-orchestrator-decision` write) by a decision against a
/// different chain on the same branch.
async fn conflict_dispatch_markers_for(
    client: &mut Client,
    repo: &str,
    dispatch_key: &str,
) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "landing_conflict_rework_dispatch"}),
        )
        .await
        .unwrap();
    res["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["payload"]["dispatch_key"].as_str() == Some(dispatch_key))
        .map(|t| t["payload"].clone())
        .collect()
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

/// The `branch_landed` half of the durable evidence
/// `LandingPipeline::hold_conflict_for_orchestrator_decision` writes for a
/// held conflict: `{merged: false, pr_opened: false}`, what
/// `reconcile::conflict_held_landing` reads to build the attention
/// violation. Mirrors `ConflictContext::dispatch_key`'s exact shape in
/// `chain_key` so a fabricated hold gets the same distinct,
/// cursor-reachable identity a real one does.
#[allow(clippy::too_many_arguments)]
async fn hold_conflict_branch_landed_only(
    client: &mut Client,
    repo: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    rework_ticket: &str,
) {
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
                    "chain_key": format!("{repo}\0{branch}\0{head_sha}\0{target}\0conflict-task\0{rework_ticket}"),
                    "detail": format!(
                        "merge conflict held for a bounded orchestrator-authority correction \
                         decision, ticket {rework_ticket}"
                    ),
                },
            }),
        )
        .await
        .unwrap();
}

/// The `landing_conflict_rework_dispatch` half of the durable evidence: a
/// marker in state `awaiting-orchestrator-decision`, what
/// `dispatch_held_conflict` reads to reconstruct its `ConflictContext` and
/// actually dispatch. Split out from [`hold_conflict_branch_landed_only`]
/// because the real `hold_conflict_for_orchestrator_decision`
/// (`landing.rs`) writes THIS marker first and the `branch_landed` event
/// second (`record_conflict_state` then `self.space.out(... "branch_landed"
/// ...)`) — a genuine window where a chain's marker exists before its own
/// `branch_landed` event does, exploitable by
/// `a_newer_chain_created_before_deciding_an_older_one_never_hijacks_its_dispatch`
/// below.
#[allow(clippy::too_many_arguments)]
async fn hold_conflict_marker_only(
    client: &mut Client,
    repo: &str,
    repo_path: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    rework_ticket: &str,
) {
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "landing_conflict_rework_dispatch",
                "lifecycle": "furniture",
                "payload": {
                    "dispatch_key": format!("{repo}\0{branch}\0{head_sha}\0{target}\0conflict-task\0{rework_ticket}"),
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

/// The exact evidence `LandingPipeline::hold_conflict_for_orchestrator_decision`
/// writes for a held conflict — both halves together, order-independent for
/// every existing test in this file (only
/// `a_newer_chain_created_before_deciding_an_older_one_never_hijacks_its_dispatch`
/// needs them split and reordered).
async fn hold_conflict(
    client: &mut Client,
    repo: &str,
    repo_path: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    rework_ticket: &str,
) {
    hold_conflict_branch_landed_only(client, repo, branch, head_sha, target, rework_ticket).await;
    hold_conflict_marker_only(
        client,
        repo,
        repo_path,
        branch,
        head_sha,
        target,
        rework_ticket,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_correction_requires_a_live_fenced_lease_and_replay_cannot_double_dispatch() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let head_sha = branch_off_main(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));
    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    hold_conflict(
        &mut client,
        repo,
        &repo_path,
        "feature",
        &head_sha,
        "main",
        "TKT-CONFLICT-CORRECTION",
    )
    .await;

    // The hold is visible through the SAME `conflict-held-landing` attention
    // item every other convergence violation surfaces through.
    let item = attention_next(&mut client, repo)
        .await
        .expect("a held conflict must surface as an attention item");
    assert_eq!(item["kind"], "conflict-held-landing");
    assert_eq!(item["subject"], "feature");
    assert_eq!(item["effective_authority"], "orchestrator");
    let item_id = item["id"].as_str().unwrap().to_string();

    // No lease has ever been acquired for this repo — refused, zero mutation.
    let no_lease = decide(&mut client, repo, &item_id, Some("orch-1"), Some(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(no_lease.contains("no lease held"), "{no_lease}");
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        0,
        "a refusal with no lease must never spawn"
    );

    // `orch-1` acquires a short-lived lease, then is superseded by `orch-2`
    // once it expires — the generation bumps, and `orch-1`'s captured
    // generation is now stale.
    let first = client
        .call(
            "lease.acquire",
            json!({"repo": repo, "holder": "orch-1", "ttl_secs": 1}),
        )
        .await
        .unwrap();
    let stale_generation = first["generation"].as_u64().unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let second = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-2"}))
        .await
        .unwrap();
    let generation = second["generation"].as_u64().unwrap();
    assert_eq!(generation, stale_generation + 1);

    let fenced = decide(
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
        agent_spawn_count(&mut client, repo).await,
        0,
        "a stale-generation decision must never spawn"
    );

    // `orch-2` holds the live, fenced lease: the decision is authorized, and
    // a real correction agent is dispatched from the held branch.
    let resolved = decide(
        &mut client,
        repo,
        &item_id,
        Some("orch-2"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(resolved["resolved"], true);
    assert_eq!(resolved["replay"], false);
    let outcome = resolved["decision"]["outcome"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(outcome.contains("TKT-CONFLICT-CORRECTION"), "{outcome}");
    assert_eq!(resolved["decision"]["kind"], "conflict-held-landing");
    assert_eq!(
        resolved["decision"]["action"],
        "conflict.dispatch_correction"
    );
    assert_eq!(resolved["decision"]["decided_by"], "orch-2");
    assert_eq!(resolved["decision"]["generation"], generation);
    assert_eq!(resolved["decision"]["attempt"], 1);
    assert_eq!(resolved["decision"]["terminal"], true);
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        1,
        "an authorized decision must dispatch exactly one correction agent"
    );

    // The branch itself is untouched — the correction is a fresh agent, not
    // an in-place mutation of the held branch or the target.
    assert_eq!(git(repo_dir.path(), &["rev-parse", "feature"]), head_sha);

    // Duplicate decision replay: a second `attention.decide` for the SAME
    // item id (a resumed orchestrator session, or a retried RPC call) must
    // return the recorded terminal decision, not dispatch a second agent.
    let replay = decide(
        &mut client,
        repo,
        &item_id,
        Some("orch-2"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay["resolved"], true);
    assert_eq!(replay["replay"], true);
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        1,
        "a replayed decision must not spawn a second correction agent"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_correction_decision_and_lease_survive_a_genuine_daemon_restart() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let head_sha = branch_off_main(repo_dir.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    // `Daemon::new` honours the configured default harness, and
    // `Config::default()` names `claude` — a binary a CI runner does not
    // have, which turned the correction dispatch below into an ENOENT
    // refusal. Every other daemon in this file comes from
    // `Daemon::new_in_memory`, whose default harness is already `fake`.
    let config = {
        let mut c = Config::default();
        c.harness.default = "fake".into();
        c.policy = allow(&["conflict-held-landing"]);
        c
    };
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));

    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();
    hold_conflict(
        &mut client,
        repo,
        &repo_path,
        "feature",
        &head_sha,
        "main",
        "TKT-CONFLICT-RESTART",
    )
    .await;

    let acquired = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = acquired["generation"].as_u64().unwrap();

    let item = attention_next(&mut client, repo)
        .await
        .expect("conflict-held-landing must surface");
    let item_id = item["id"].as_str().unwrap().to_string();
    let decided = decide(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(decided["resolved"], true);
    assert_eq!(agent_spawn_count(&mut client, repo).await, 1);

    let lease_before_restart = client
        .call(
            "lease.renew",
            json!({"repo": repo, "holder": "orch-1", "generation": generation}),
        )
        .await
        .unwrap();
    let cursor = lease_before_restart["cursor"]
        .as_str()
        .expect("cursor must have advanced past the decided item")
        .to_string();
    assert_eq!(cursor, item_id);

    // Genuinely kill and replace the daemon process — the lease store's and
    // the decision journal's crash-safety claims are specifically about
    // surviving that, not merely a dropped in-memory `Space`.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The lease resumes with its generation and cursor untouched.
    let resumed = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(resumed["generation"].as_u64().unwrap(), generation);
    assert_eq!(resumed["cursor"].as_str().unwrap(), cursor);

    // A replayed decide for the same item, across the restart, still
    // returns the terminal record rather than re-dispatching.
    let replay = decide(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay["resolved"], true);
    assert_eq!(replay["replay"], true);
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        1,
        "a post-restart replay must not spawn a second correction agent"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
    handle_b.abort();
    let _ = handle_b.await;
}

/// TKT-01M0Q95QVGNE8ZABH5Z1WQB3J0: a correction that lands back on the held
/// branch can itself conflict again — a genuinely SECOND, later chain on the
/// exact same (repo, branch). `reconcile::conflict_held_landing` must give
/// it its own attention/decision identity (not replay or permanently shadow
/// the first chain's terminal decision — see the `reconcile.rs` fix that
/// anchors this id to the land tuple's own monotonic `RecordId`), and each
/// chain must still only ever dispatch once, independent of the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_sequential_conflict_chains_on_the_same_branch_each_lease_exactly_once() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let head_sha_1 = branch_off_main(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));
    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    // Chain one: held, then dispatched under a live lease.
    hold_conflict(
        &mut client,
        repo,
        &repo_path,
        "feature",
        &head_sha_1,
        "main",
        "TKT-CHAIN-ONE",
    )
    .await;
    let item1 = attention_next(&mut client, repo)
        .await
        .expect("the first chain must surface");
    let item1_id = item1["id"].as_str().unwrap().to_string();

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    let decided1 = decide_with_budget(
        &mut client,
        repo,
        &item1_id,
        Some("orch-1"),
        Some(generation),
        Some(2.5),
        Some(10_000),
    )
    .await
    .unwrap();
    assert_eq!(decided1["resolved"], true);
    assert_eq!(decided1["replay"], false);
    // The full decision-journal envelope a valid, fenced dispatch must
    // record — holder, generation, authority, action, bounded evidence
    // references, attempt, budget, outcome, and terminal state — not just
    // the handful `resolved`/`replay` on the RPC response's own top level.
    let decision1 = &decided1["decision"];
    assert_eq!(decision1["decided_by"], "orch-1");
    assert_eq!(decision1["generation"], generation);
    assert_eq!(decision1["authority"], "orchestrator");
    assert_eq!(decision1["action"], "conflict.dispatch_correction");
    assert_eq!(decision1["attempt"], 1);
    assert_eq!(decision1["budget_usd"], 2.5);
    assert_eq!(decision1["budget_tokens"], 10_000);
    assert_eq!(decision1["terminal"], true);
    let evidence1 = decision1["evidence"]
        .as_array()
        .expect("evidence must be a bounded list of references, not an unbounded blob");
    assert!(
        !evidence1.is_empty() && evidence1.len() <= 8,
        "evidence must be a short, bounded list of references: {evidence1:?}"
    );
    assert!(
        evidence1
            .iter()
            .any(|e| e.as_str().is_some_and(|s| s.starts_with("branch_landed:"))),
        "evidence must reference the exact branch_landed event that proves the hold: {evidence1:?}"
    );
    let outcome1 = decision1["outcome"].as_str().unwrap().to_string();
    assert!(outcome1.contains("TKT-CHAIN-ONE"), "{outcome1}");
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        1,
        "chain one must dispatch exactly one correction agent"
    );

    // Chain two: the correction lands back on `feature`, conflicts again at
    // a genuinely new head — a second, distinct hold on the SAME branch.
    let head_sha_2 = advance_branch(repo_dir.path(), "feature", "feature-2.txt");
    hold_conflict(
        &mut client,
        repo,
        &repo_path,
        "feature",
        &head_sha_2,
        "main",
        "TKT-CHAIN-TWO",
    )
    .await;
    let item2 = attention_next(&mut client, repo).await.expect(
        "a second, later conflict on the same branch must surface past the first chain's \
             cursor, not be permanently hidden behind it",
    );
    let item2_id = item2["id"].as_str().unwrap().to_string();
    assert_ne!(
        item1_id, item2_id,
        "sequential conflicts on the same branch must never share an attention/decision identity"
    );

    let decided2 = decide(
        &mut client,
        repo,
        &item2_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(decided2["resolved"], true);
    assert_eq!(decided2["replay"], false);
    let outcome2 = decided2["decision"]["outcome"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(outcome2.contains("TKT-CHAIN-TWO"), "{outcome2}");
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        2,
        "chain two must dispatch its own correction agent, independent of chain one"
    );

    // Replay inside EITHER chain must not double-dispatch.
    let replay1 = decide(
        &mut client,
        repo,
        &item1_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay1["replay"], true);
    let replay2 = decide(
        &mut client,
        repo,
        &item2_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay2["replay"], true);
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        2,
        "replaying either chain's decision must never spawn a second agent for it"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A durable `"dispatching"` marker with no journaled worker looks identical
/// to a fresh, never-attempted hold UNLESS `dispatch_held_conflict` checks
/// the supervisor's own registry — the exact ambiguity `landing.rs`'s
/// `dispatch_held_conflict` match on `state` resolves by failing closed with
/// `"dispatch-interrupted"` rather than guessing. This never reaches
/// `route_conflict`/`hold_conflict_for_orchestrator_decision` at all (no
/// `agent.spawn` in this daemon's fake-harness env is configured), so the
/// marker is pre-seeded directly in state `"dispatching"`, as a genuine
/// daemon crash between recording that state and the spawn journaling would
/// leave it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupted_dispatching_marker_fails_closed_and_stays_unresolved_on_retry() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let head_sha = branch_off_main(repo_dir.path());

    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    // The durable evidence a real `route_conflict` writes, EXCEPT the
    // dispatch marker is seeded straight into `"dispatching"` — no
    // `"awaiting-orchestrator-decision"` middle step — with no corresponding
    // rat ever registered in the supervisor. This is precisely the crash
    // window: a daemon that recorded "dispatching" and died before either
    // the spawn call or the terminal "dispatched" marker landed.
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "scope": repo,
                "identity": "branch_landed",
                "payload": {
                    "branch": "feature",
                    "target": "main",
                    "merged": false,
                    "pr_opened": false,
                    "chain_key": format!("{repo}\0feature\0{head_sha}\0main\0conflict-task\0TKT-INTERRUPTED"),
                    "detail": "merge conflict held for a bounded orchestrator-authority correction decision, ticket TKT-INTERRUPTED",
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
                    "dispatch_key": format!("{repo}\0feature\0{head_sha}\0main\0conflict-task\0TKT-INTERRUPTED"),
                    "repo": repo,
                    "repo_path": &repo_path,
                    "source": "feature",
                    "branch": "feature",
                    "head_sha": head_sha,
                    "target": "main",
                    "target_head": "target-head-placeholder",
                    "fork_point": "fork-point-placeholder",
                    "task": "conflict-task",
                    "rework_ticket": "TKT-INTERRUPTED",
                    "conflict_evidence": "CONFLICT (content): Merge conflict in feature.txt",
                    "agent": Value::Null,
                    "attempt": 1,
                    "state": "dispatching",
                    "diff_files": 1,
                    "diff_lines": 1,
                },
            }),
        )
        .await
        .unwrap();

    let item = attention_next(&mut client, repo)
        .await
        .expect("an interrupted dispatch is still a held conflict-held-landing item");
    let item_id = item["id"].as_str().unwrap().to_string();

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();
    assert_eq!(
        lease["cursor"],
        Value::Null,
        "a fresh lease starts with no cursor"
    );

    // First decide: hits the crash-window arm. Must fail closed, not report
    // phantom success, and must never spawn (no fake harness env is even
    // configured for this test).
    let first = decide(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        first.contains("the supervisor registry contains no rat for it")
            && first.contains("daemon may have stopped between recording the marker"),
        "{first}"
    );
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        0,
        "an interrupted dispatch must never spawn a correction agent"
    );

    // The lease cursor must NOT have advanced past this violation — a
    // failed, non-terminal attempt resolved nothing.
    let after_first = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(
        after_first["cursor"],
        Value::Null,
        "a failed, non-terminal decision must never advance the lease cursor"
    );

    // Retry: `dispatch_held_conflict` now sees the marker it just wrote in
    // state `"dispatch-interrupted"` — a terminal human gate — and must
    // refuse again rather than silently converging on success just because
    // it is being asked a second time.
    let second = decide(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        second.contains("already terminally") && second.contains("dispatch-interrupted"),
        "{second}"
    );
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        0,
        "a retry against an unresolved human gate must never spawn either"
    );
    let after_second = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(
        after_second["cursor"],
        Value::Null,
        "the cursor stays put across a retried, still-unresolved gate"
    );
}

/// Proves the "bind conflict-correction dispatch to the exact decided
/// chain" fix (c36444f) end-to-end: a genuinely NEWER conflict chain (B)
/// can have its dispatch marker recorded on the same (repo, branch) BEFORE
/// an OLDER chain's (A) decision is made — the exact window the real
/// `hold_conflict_for_orchestrator_decision` (`landing.rs`) itself creates,
/// since it writes a chain's `landing_conflict_rework_dispatch` marker
/// BEFORE its `branch_landed` event, not atomically with it. Without
/// `chain_key` threaded from the deciding violation's own evidence all the
/// way through to `dispatch_held_conflict`
/// (`landing.rs::conflict_marker_for_chain_key`), "whichever chain is
/// latest for this branch right now" would resolve A's decision against
/// B's marker instead — silently dispatching the wrong chain's correction
/// under a decision that named a different one, while A's own marker sits
/// forever unresolved.
///
/// Chain B's marker deliberately reuses chain A's own `head_sha` (differing
/// only by `rework_ticket`, hence still a distinct `chain_key`): the point
/// under test is exact-CHAIN selection, not the unrelated
/// `"conflicted-head-moved"` tip check every dispatch must also pass, and
/// giving both chains the same real branch tip removes that confound so a
/// wrong-chain dispatch shows up as a clean (wrong-ticket) SUCCESS rather
/// than an ambiguous failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newer_chain_created_before_deciding_an_older_one_never_hijacks_its_dispatch() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let head_sha = branch_off_main(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));
    let mut client = daemon_with_policy(home.path(), allow(&["conflict-held-landing"])).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    let repo_path = repo_dir.path().to_string_lossy().to_string();
    client
        .call("repo.add", json!({"name": repo, "path": &repo_path}))
        .await
        .unwrap();

    // Chain A: held in full — marker AND branch_landed — so it surfaces as
    // an attention item.
    hold_conflict(
        &mut client,
        repo,
        &repo_path,
        "feature",
        &head_sha,
        "main",
        "TKT-CHAIN-A",
    )
    .await;
    let item_a = attention_next(&mut client, repo)
        .await
        .expect("chain A must surface");
    let item_a_id = item_a["id"].as_str().unwrap().to_string();

    // Chain B: ONLY its dispatch marker exists so far — no branch_landed
    // event yet, so `attention.decide`'s report snapshot (built from
    // branch_landed events, deduped to one per branch) still sees ONLY
    // chain A. `dispatch_held_conflict`'s own, separate marker scan (over
    // `landing_conflict_rework_dispatch` events directly) is NOT deduped
    // this way, and sees B's marker too — exactly the asymmetry the
    // TOCTOU fix closes.
    let chain_key_b = format!("{repo}\0feature\0{head_sha}\0main\0conflict-task\0TKT-CHAIN-B");
    hold_conflict_marker_only(
        &mut client,
        repo,
        &repo_path,
        "feature",
        &head_sha,
        "main",
        "TKT-CHAIN-B",
    )
    .await;

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = lease["generation"].as_u64().unwrap();

    // Deciding A must dispatch A's OWN correction, never B's, even though
    // B's marker was written more recently and is the "latest" marker for
    // this branch by every ordinary newest-wins read.
    let decided_a = decide(
        &mut client,
        repo,
        &item_a_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(decided_a["resolved"], true);
    assert_eq!(decided_a["replay"], false);
    let decision_a = &decided_a["decision"];
    let outcome_a = decision_a["outcome"].as_str().unwrap().to_string();
    assert!(outcome_a.contains("TKT-CHAIN-A"), "{outcome_a}");
    assert!(!outcome_a.contains("TKT-CHAIN-B"), "{outcome_a}");
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        1,
        "chain A's decision must dispatch exactly one agent"
    );
    assert_eq!(
        agent_spawned_tasks(&mut client, repo).await,
        vec!["TKT-CHAIN-A".to_string()],
        "the spawned correction must be for chain A, not the newer chain B"
    );

    // Chain B's own marker must be completely untouched by A's decision —
    // still its original single "awaiting-orchestrator-decision" write, not
    // silently advanced nor gated with a spurious withhold.
    let markers_b = conflict_dispatch_markers_for(&mut client, repo, &chain_key_b).await;
    assert_eq!(
        markers_b.len(),
        1,
        "chain B must be untouched by chain A's decision: {markers_b:?}"
    );
    assert_eq!(markers_b[0]["state"], "awaiting-orchestrator-decision");

    // Now chain B's branch_landed event lands too (the "human's push"
    // catching up in the tuplespace) — it becomes independently pending and
    // decidable, on its own distinct identity, past chain A's now-advanced
    // lease cursor.
    hold_conflict_branch_landed_only(
        &mut client,
        repo,
        "feature",
        &head_sha,
        "main",
        "TKT-CHAIN-B",
    )
    .await;
    let item_b = attention_next(&mut client, repo)
        .await
        .expect("chain B must surface once its own branch_landed event exists");
    let item_b_id = item_b["id"].as_str().unwrap().to_string();
    assert_ne!(
        item_a_id, item_b_id,
        "chains A and B must never share an attention/decision identity"
    );

    let decided_b = decide(
        &mut client,
        repo,
        &item_b_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(decided_b["resolved"], true);
    assert_eq!(decided_b["replay"], false);
    let outcome_b = decided_b["decision"]["outcome"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(outcome_b.contains("TKT-CHAIN-B"), "{outcome_b}");
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        2,
        "chain B's own decision must dispatch its own, independent correction agent"
    );
    let spawns_after_b = agent_spawned_tasks(&mut client, repo).await;
    assert_eq!(
        spawns_after_b
            .iter()
            .filter(|t| t.as_str() == "TKT-CHAIN-A")
            .count(),
        1
    );
    assert_eq!(
        spawns_after_b
            .iter()
            .filter(|t| t.as_str() == "TKT-CHAIN-B")
            .count(),
        1
    );

    // Replaying EITHER chain's decision must never double-spawn.
    let replay_a = decide(
        &mut client,
        repo,
        &item_a_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay_a["replay"], true);
    let replay_b = decide(
        &mut client,
        repo,
        &item_b_id,
        Some("orch-1"),
        Some(generation),
    )
    .await
    .unwrap();
    assert_eq!(replay_b["replay"], true);
    assert_eq!(
        agent_spawn_count(&mut client, repo).await,
        2,
        "replaying either chain's decision must never spawn a second agent for it"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
