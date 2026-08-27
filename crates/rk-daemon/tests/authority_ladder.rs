//! End-to-end coverage for TKT-01M0E8PN9C41BWECGNW0990R3J's unattended
//! authority ladder: `lease.acquire`/`lease.renew`, `attention.next`/
//! `attention.decide`, dispatched by a castle's own declared `[policy]`.
//!
//! Every acceptance-criteria bullet gets its own proof here (the pure
//! matrix/policy-narrowing unit tests already live in
//! `rk-daemon/src/authority.rs`, `rk-daemon/src/attention.rs`, and
//! `rk-daemon/src/reconcile.rs`; `rk-core::config::tests::
//! a_declared_policy_file_narrows_and_allowlists_exactly_what_it_names`
//! covers the file-parsing half of "policy declares the matrix"):
//!
//! - a mechanical fixture repairs with no LLM/lease at all, and its journal
//!   shows the durable intent-then-terminal write order.
//! - a human fixture is REFUSED (`codes::FORBIDDEN`) with ZERO mutation and
//!   NO tuple written at all — the refusal message itself carries the
//!   requested decision, blast radius, and resolving action, but nothing is
//!   journaled, so a replayed call simply refuses again rather than echoing
//!   a durable record.
//! - an orchestrator fixture is resolved only through a live, fenced lease,
//!   gated by the castle's own `orchestrator_action_allowlist` — refused
//!   with zero mutation when that list is empty (the conservative default),
//!   even holding a valid lease.
//! - a rate-held orchestrator decision is NOT journaled as resolved: it
//!   stays retryable after the window resets rather than being permanently
//!   poisoned as "already decided".
//! - a resumed `attention.decide` call for a violation whose underlying data
//!   already changed out-of-band (modelling a crash between a prior
//!   attempt's mutation and its own journal write) converges safely with no
//!   further mutation, rather than erroring or re-mutating.
//! - replaying any of the above cannot repeat the mutation.
//! - the lease and its cursor survive a disconnect (same holder
//!   re-acquiring), a holder replacement (a different holder after
//!   expiry), and a genuine daemon restart.
//! - a REAL declared `config.toml` — not `PolicyConfig::default()` in Rust —
//!   is what governs the RPC boundary.
//! - there is no RPC method an orchestrator session could call to widen its
//!   own authority.

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

/// `RK_FAKE_HARNESS_CMD` is process-global; every test in this file that
/// spawns an agent uses the SAME completion script, so this only guards
/// against a concurrent set/unset race, not content drift (see
/// `agent_lifecycle.rs`'s identical guard).
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const QUICK_DONE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"authority-ladder-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"authority-ladder-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
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

/// `rk_git::Repo::name()` (what `agent.spawn` stamps onto
/// `AgentRecord::repo_name`, read by `reconcile::build`) is ALWAYS the
/// worktree directory's basename, independent of whatever custom `name` a
/// test's own `repo.add` call used. A fixture that spawns an agent has to
/// key its ticket scope and `repo.add` registration off this SAME basename
/// or the agent record and the ticket end up in what `reconcile::build`
/// treats as two different repos, and the violation never surfaces.
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

fn ticket_payload(status: &str, extra: Value) -> Value {
    let mut payload = json!({"title": "t", "status": status, "assignee": Value::Null});
    if let (Some(obj), Some(extra_obj)) = (payload.as_object_mut(), extra.as_object()) {
        for (k, v) in extra_obj {
            obj.insert(k.clone(), v.clone());
        }
    }
    payload
}

fn delivery(commit: &str, branch: &str, target: &str) -> Value {
    json!({
        "delivery": {
            "merge_commit": commit,
            "branch": branch,
            "target": target,
            "landed_at": "2026-08-19T00:00:00Z",
        }
    })
}

/// Write a `task` tuple directly (bypassing `ticket.new`) so a fixture can
/// set exactly the fields `crate::reconcile::build` inspects — a delivery
/// record, an assignee, a status — without a full spawn/land cycle. Mirrors
/// `reconcile_report.rs`'s own use of raw `space.out` for its `branch_landed`
/// fixture.
async fn write_ticket(client: &mut Client, repo: &str, id: &str, payload: Value) {
    client
        .call(
            "space.out",
            json!({
                "category": "task",
                "scope": repo,
                "identity": id,
                "payload": payload,
                "lifecycle": "session",
            }),
        )
        .await
        .unwrap();
}

async fn get_ticket(client: &mut Client, id: &str) -> Value {
    let res = client.call("ticket.get", json!({"id": id})).await.unwrap();
    res["ticket"].clone()
}

/// `attention.next` walks ONE shared cursor across every kind in the report,
/// which is fine for a fixture with a single violation but not for a test
/// that deliberately puts two different kinds in the same repo (a human item
/// that never advances anything sorting ahead of the orchestrator item this
/// helper actually wants). `reconcile.report` has no cursor at all — this
/// reads the violation directly by kind/subject, the same way an operator
/// reading `rk reconcile report` would pick one row out of several.
async fn reconcile_violation(client: &mut Client, repo: &str, kind: &str, subject: &str) -> Value {
    let report = client
        .call("reconcile.report", json!({"repo": repo}))
        .await
        .unwrap();
    report["violations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["kind"] == kind && v["subject"] == subject)
        .unwrap_or_else(|| panic!("no {kind} violation for {subject}: {report}"))
        .clone()
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

/// Every decision-journal event recorded for `violation_id`, oldest scan
/// order — used to prove the durable intent-before-mutation write order
/// directly, not just its end effect.
async fn decision_events(client: &mut Client, repo: &str, violation_id: &str) -> Vec<Value> {
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
        .filter(|t| t["payload"]["violation_id"] == violation_id)
        .cloned()
        .collect()
}

/// Spawn a fake-harness rat that completes almost immediately and wait for
/// it to reach `Completed` — a terminal `AgentState`
/// (`crate::agents::AgentState::is_archivable`) — returning its name.
async fn spawn_completed_agent(client: &mut Client, repo_dir: &Path, task: &str) -> String {
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.to_string_lossy(),
                "task": task,
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let status = client
            .call("agent.status", json!({"name": &name}))
            .await
            .unwrap();
        match status["agent"]["state"].as_str() {
            Some("completed") => return name,
            Some("failed") => panic!("fake agent failed instead of completing: {status}"),
            _ => {}
        }
    }
    panic!("fake agent never completed");
}

async fn daemon_with_policy(home: &Path, cfg: PolicyConfig) -> Client {
    let layout = Layout::at(home);
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_authority_policy_for_tests(&cfg);
    tokio::spawn(daemon.run());
    connect(&layout).await
}

fn allow(kinds: &[&str]) -> PolicyConfig {
    PolicyConfig {
        orchestrator_action_allowlist: kinds.iter().map(|s| s.to_string()).collect(),
        ..PolicyConfig::default()
    }
}

// ---------------------------------------------------------------------
// Mechanical: repairs with no LLM, no lease.
// ---------------------------------------------------------------------

#[tokio::test]
async fn mechanical_fixture_repairs_without_an_llm_and_replay_cannot_repeat_it() {
    let home = tempfile::tempdir().unwrap();
    // `execute_mechanical` now routes `delivered-but-open` through
    // `reconcile_repair::plan`, which needs the SAME durable git evidence
    // `reconcile.repair` does (ancestry, protected-path touch) — an
    // unregistered repo reads as `HoldReason::MissingEvidence`, never a
    // clean bill. A real repo, a real registration, and a real landed
    // commit keep this fixture's fail-closed evidence model honest instead
    // of special-casing a no-git path around it.
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    std::fs::write(repo_dir.path().join("mech.txt"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "mech work"]);
    let merge_commit = git(repo_dir.path(), &["rev-parse", "HEAD"]);

    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;
    let repo = "mech-repo";
    client
        .call(
            "repo.add",
            json!({"name": repo, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    write_ticket(
        &mut client,
        repo,
        "TKT-MECH-1",
        ticket_payload(
            "in_progress",
            delivery(&merge_commit, "rat/x/tkt-mech-1", "main"),
        ),
    )
    .await;

    let item = attention_next(&mut client, repo)
        .await
        .expect("delivered-but-open must surface as an attention item");
    assert_eq!(item["kind"], "delivered-but-open");
    assert_eq!(item["effective_authority"], "mechanical");
    let item_id = item["id"].as_str().unwrap().to_string();

    // No `holder`/`generation`/lease at all — the whole point of Mechanical
    // authority.
    let result = decide(&mut client, repo, &item_id, None, None)
        .await
        .unwrap();
    assert_eq!(result["resolved"], true);
    assert_eq!(result["replay"], false);
    assert_eq!(result["decision"]["decided_by"], "mechanical");
    assert_eq!(result["decision"]["action"], "ticket.set_status(closed)");

    let ticket = get_ticket(&mut client, "TKT-MECH-1").await;
    assert_eq!(ticket["payload"]["status"], "closed");

    // The durable intent-before-mutation write: the journal for this
    // violation must show a non-terminal "attempting" record BEFORE the
    // terminal, resolved one — proving the mutation was never executed
    // without a preceding durable trace.
    // `space.scan` makes no ordering guarantee, so find each record by its
    // own shape rather than assuming insertion order.
    let events = decision_events(&mut client, repo, &item_id).await;
    assert_eq!(
        events.len(),
        2,
        "expected an intent record and a terminal record: {events:?}"
    );
    let intent = events
        .iter()
        .find(|e| e["payload"]["terminal"] == false)
        .unwrap_or_else(|| panic!("no intent record: {events:?}"));
    assert_eq!(intent["payload"]["outcome"], "attempting");
    let terminal = events
        .iter()
        .find(|e| e["payload"]["terminal"] == true)
        .unwrap_or_else(|| panic!("no terminal record: {events:?}"));
    assert_eq!(terminal["payload"]["resolved"], true);

    let closed_at = ticket["payload"]["updated_at"].clone();

    // Replay: `delivered-but-open` is self-clearing (closing the ticket is
    // exactly what makes `reconcile::build` stop raising it), but the
    // durable decision journal is consulted BEFORE a fresh report is ever
    // built, so a captured id still resolves to the SAME terminal record as
    // `replay: true` rather than converging on "not found" — which would
    // otherwise hide a genuine replay behind "this violation doesn't exist
    // any more" and never actually exercise the no-second-mutation guarantee.
    let replay = decide(&mut client, repo, &item_id, None, None)
        .await
        .unwrap();
    assert_eq!(replay["resolved"], true);
    assert_eq!(replay["replay"], true);
    assert_eq!(replay["decision"]["decided_by"], "mechanical");

    let ticket_again = get_ticket(&mut client, "TKT-MECH-1").await;
    assert_eq!(
        ticket_again["payload"]["updated_at"], closed_at,
        "a replay must not touch the ticket a second time"
    );
    let events_after_replay = decision_events(&mut client, repo, &item_id).await;
    assert_eq!(
        events_after_replay.len(),
        2,
        "a replay must not write another decision-journal event"
    );
}

// ---------------------------------------------------------------------
// Mechanical: resolving one ticket's violation must never repair a sibling
// ticket that is independently eligible for the same repair kind.
// `execute_mechanical` builds its repair plan over the WHOLE repo (the same
// scan `reconcile.repair` uses) and must narrow it to exactly the one
// violation named by the decided item before applying.
// ---------------------------------------------------------------------

#[tokio::test]
async fn mechanical_repair_does_not_touch_a_sibling_ticket_with_the_same_kind() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    // Two independent commits landed on `main`, each the sole delivery
    // evidence for a DIFFERENT ticket — both eligible for the mechanical
    // `delivered-but-open` repair at the same time.
    std::fs::write(repo_dir.path().join("sib-a.txt"), "a\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "sib a"]);
    let commit_a = git(repo_dir.path(), &["rev-parse", "HEAD"]);

    std::fs::write(repo_dir.path().join("sib-b.txt"), "b\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "sib b"]);
    let commit_b = git(repo_dir.path(), &["rev-parse", "HEAD"]);

    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;
    let repo = "mech-siblings";
    client
        .call(
            "repo.add",
            json!({"name": repo, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    write_ticket(
        &mut client,
        repo,
        "TKT-SIB-A",
        ticket_payload(
            "in_progress",
            delivery(&commit_a, "rat/x/tkt-sib-a", "main"),
        ),
    )
    .await;
    write_ticket(
        &mut client,
        repo,
        "TKT-SIB-B",
        ticket_payload(
            "in_progress",
            delivery(&commit_b, "rat/x/tkt-sib-b", "main"),
        ),
    )
    .await;

    // `attention.next`'s shared cursor would only ever surface one of these
    // at a time; read A's violation directly so B is untouched by the query
    // itself, not just by the decision under test.
    let violation_a =
        reconcile_violation(&mut client, repo, "delivered-but-open", "TKT-SIB-A").await;
    let result = decide(
        &mut client,
        repo,
        violation_a["id"].as_str().unwrap(),
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(result["resolved"], true);

    let ticket_a = get_ticket(&mut client, "TKT-SIB-A").await;
    assert_eq!(ticket_a["payload"]["status"], "closed");

    // The sibling — independently eligible for the exact same repair kind —
    // must be completely untouched by resolving A's own violation.
    let ticket_b = get_ticket(&mut client, "TKT-SIB-B").await;
    assert_eq!(
        ticket_b["payload"]["status"], "in_progress",
        "resolving one ticket's delivered-but-open violation must not repair a sibling"
    );

    // No repair-applied journal entry exists for B's own violation id: the
    // plan `execute_mechanical` applied must have contained only A's item.
    let applied = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "reconcile_repair_applied"}),
        )
        .await
        .unwrap();
    let violation_b_id = "delivered-but-open:TKT-SIB-B";
    let touched_b = applied["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .any(|t| t["payload"]["violation_id"] == violation_b_id);
    assert!(
        !touched_b,
        "sibling ticket's own violation must never be marked repaired: {applied}"
    );
}

// ---------------------------------------------------------------------
// Human: refused with zero mutation, but a durable, actionable gate.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn human_fixture_is_refused_with_zero_mutation_and_no_tuple_written() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    // A commit that only ever exists on `side`, never merged into `main`.
    git(repo_dir.path(), &["checkout", "-b", "side"]);
    std::fs::write(repo_dir.path().join("f.txt"), "x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "side work"]);
    let side_sha = git(repo_dir.path(), &["rev-parse", "HEAD"]);
    git(repo_dir.path(), &["checkout", "main"]);

    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;
    let repo_name = "human-repo".to_string();
    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // status "closed": `delivered_but_open` explicitly excludes done/closed
    // tickets, so this ticket raises ONLY tracker-contradicts-git-history —
    // any other open-ish status would ALSO raise delivered-but-open (it
    // never checks git, only "does a delivery record exist and is the
    // ticket still open"), and that sorts first, hiding the fixture this
    // test actually wants behind it.
    write_ticket(
        &mut client,
        &repo_name,
        "TKT-HUMAN-1",
        ticket_payload("closed", delivery(&side_sha, "side", "main")),
    )
    .await;

    let item = attention_next(&mut client, &repo_name)
        .await
        .expect("a delivery record git disagrees with must surface");
    assert_eq!(item["kind"], "tracker-contradicts-git-history");
    assert_eq!(item["effective_authority"], "human");
    let item_id = item["id"].as_str().unwrap().to_string();

    let before = get_ticket(&mut client, "TKT-HUMAN-1").await;

    let err = decide(&mut client, &repo_name, &item_id, None, None)
        .await
        .expect_err("a human gate must be refused (FORBIDDEN), never a successful response")
        .to_string();
    assert!(err.contains(&item_id), "{err}");
    assert!(err.contains("human-gated"), "{err}");
    // The refusal message itself carries what a human must decide, what is
    // at stake, and how to resolve it — even though nothing is journaled.
    assert!(
        err.contains("delivery record") || err.contains("ancestry"),
        "expected the gate's requested-decision text in the refusal: {err}"
    );

    // ZERO mutation: the ticket is byte-identical to before the decide call.
    let after = get_ticket(&mut client, "TKT-HUMAN-1").await;
    assert_eq!(before, after, "a human gate must never touch the ticket");

    // ZERO tuple written: no decision-journal entry at all for this
    // violation id — a human gate is the one arm of `attention.decide` that
    // never reaches the journal write.
    let events = decision_events(&mut client, &repo_name, &item_id).await;
    assert!(
        events.is_empty(),
        "a human gate must write no tuple: {events:?}"
    );

    // Replaying refuses again the same way — there is nothing durable to
    // replay (nothing was ever journaled), so this re-evaluates fresh and
    // is refused fresh, not "replay: true" against a stored record.
    let err_again = decide(&mut client, &repo_name, &item_id, None, None)
        .await
        .expect_err("a replayed human gate must also be refused")
        .to_string();
    assert!(err_again.contains("human-gated"), "{err_again}");
    let after_replay = get_ticket(&mut client, "TKT-HUMAN-1").await;
    assert_eq!(before, after_replay);
    let events_after_replay = decision_events(&mut client, &repo_name, &item_id).await;
    assert!(events_after_replay.is_empty());

    // The daily surface turns this otherwise-unbounded human gate into one
    // explicit, supported disposition command. It does not expose the old
    // lease/cursor machinery to the operator.
    let work_before = client
        .call("work.current", json!({"repo": &repo_name}))
        .await
        .unwrap();
    let current = work_before["attention"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == item_id)
        .expect("human contradiction must be current actionable attention");
    assert_eq!(
        current["command"],
        format!("rk attention invalidate {repo_name} {item_id}")
    );

    let invalidated = client
        .call(
            "attention.invalidate",
            json!({
                "repo": &repo_name,
                "item": &item_id,
                "reason": "operator confirmed the historical delivery claim is irrelevant",
            }),
        )
        .await
        .unwrap();
    assert_eq!(invalidated["resolved"], true);
    assert_eq!(invalidated["replay"], false);
    assert_eq!(invalidated["decision"]["action"], "attention.invalidate");
    assert_eq!(invalidated["decision"]["terminal"], true);
    assert_eq!(
        get_ticket(&mut client, "TKT-HUMAN-1").await,
        before,
        "invalidation settles attention only; it must not rewrite the fact"
    );
    assert!(
        attention_next(&mut client, &repo_name).await.is_none(),
        "terminal invalidation must disappear from the live attention queue"
    );
    let work_after = client
        .call("work.current", json!({"repo": &repo_name}))
        .await
        .unwrap();
    assert!(work_after["attention"].as_array().unwrap().is_empty());

    // Repeating the same disposition replays the one durable record and
    // never creates a second decision.
    let replay = client
        .call(
            "attention.invalidate",
            json!({"repo": &repo_name, "item": &item_id}),
        )
        .await
        .unwrap();
    assert_eq!(replay["resolved"], true);
    assert_eq!(replay["replay"], true);
    assert_eq!(
        decision_events(&mut client, &repo_name, &item_id)
            .await
            .len(),
        1
    );
}

// ---------------------------------------------------------------------
// Orchestrator: refused with zero mutation when its kind is not
// explicitly allowlisted, even holding a live lease.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_authority_requires_an_allowlisted_kind_even_with_a_live_lease() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));
    // Conservative default: `orchestrator_action_allowlist` is empty.
    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    client
        .call(
            "repo.add",
            json!({"name": repo, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let owner = spawn_completed_agent(&mut client, repo_dir.path(), "orch-deny").await;
    write_ticket(
        &mut client,
        repo,
        "TKT-ORCH-DENY-1",
        ticket_payload("in_progress", json!({"assignee": owner})),
    )
    .await;

    let item = attention_next(&mut client, repo)
        .await
        .expect("a terminal-owned in-progress ticket must surface");
    assert_eq!(item["kind"], "terminal-assignee-active-work");
    assert_eq!(item["effective_authority"], "orchestrator");
    let item_id = item["id"].as_str().unwrap().to_string();

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();

    let err = decide(
        &mut client,
        repo,
        &item_id,
        Some("orch-1"),
        Some(lease["generation"].as_u64().unwrap()),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("orchestrator_action_allowlist"), "{err}");

    let ticket = get_ticket(&mut client, "TKT-ORCH-DENY-1").await;
    assert_eq!(
        ticket["payload"]["status"], "in_progress",
        "a refused orchestrator decision must not mutate the ticket"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// ---------------------------------------------------------------------
// Orchestrator: resolved only through a live, fenced lease; a rate-held
// decision is not journaled as resolved and stays retryable.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orchestrator_fixture_resolves_through_the_lease_and_a_rate_held_decision_stays_retryable()
{
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));
    let mut cfg = allow(&["terminal-assignee-active-work"]);
    cfg.orchestrator_rate_cap = 1;
    cfg.orchestrator_rate_window_secs = 1;
    let mut client = daemon_with_policy(home.path(), cfg).await;
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();
    client
        .call(
            "repo.add",
            json!({"name": repo, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let owner_a = spawn_completed_agent(&mut client, repo_dir.path(), "orch-a").await;
    let owner_b = spawn_completed_agent(&mut client, repo_dir.path(), "orch-b").await;
    write_ticket(
        &mut client,
        repo,
        "TKT-ORCH-A",
        ticket_payload("in_progress", json!({"assignee": owner_a})),
    )
    .await;
    write_ticket(
        &mut client,
        repo,
        "TKT-ORCH-B",
        ticket_payload("in_progress", json!({"assignee": owner_b})),
    )
    .await;

    let lease = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(lease["generation"], 1);
    assert_eq!(lease["cursor"], Value::Null);
    let generation = lease["generation"].as_u64().unwrap();

    // First orchestrator decision this rolling window: succeeds, consumes
    // the single rate-cap slot, and resolves through the lease.
    let item_a = attention_next(&mut client, repo).await.unwrap();
    let id_a = item_a["id"].as_str().unwrap().to_string();
    let result_a = decide(&mut client, repo, &id_a, Some("orch-1"), Some(generation))
        .await
        .unwrap();
    assert_eq!(result_a["resolved"], true);
    assert_eq!(result_a["held"], false);
    let ticket_a = get_ticket(&mut client, "TKT-ORCH-A").await;
    assert_eq!(ticket_a["payload"]["status"], "open");

    // The lease's cursor advanced past item_a's own id.
    let lease_after_a = client
        .call(
            "lease.renew",
            json!({"repo": repo, "holder": "orch-1", "generation": generation}),
        )
        .await
        .unwrap();
    assert_eq!(lease_after_a["cursor"], json!(id_a));

    // Second, DIFFERENT violation: same fleet-wide "orchestrator-action"
    // rate bucket is now exhausted, so this is held — with ZERO mutation.
    let item_b = attention_next(&mut client, repo)
        .await
        .expect("item_b must still be reachable even though item_a's cursor advanced past it");
    let id_b = item_b["id"].as_str().unwrap().to_string();
    assert_ne!(id_a, id_b);
    let held = decide(&mut client, repo, &id_b, Some("orch-1"), Some(generation))
        .await
        .unwrap();
    assert_eq!(held["resolved"], false);
    assert_eq!(held["held"], true);
    let ticket_b_held = get_ticket(&mut client, "TKT-ORCH-B").await;
    assert_eq!(
        ticket_b_held["payload"]["status"], "in_progress",
        "a rate-held decision must not mutate the ticket"
    );

    // Immediately retrying (still inside the same window) is NOT treated as
    // an already-resolved replay: it re-evaluates fresh and is held again,
    // not silently reported as done.
    let held_again = decide(&mut client, repo, &id_b, Some("orch-1"), Some(generation))
        .await
        .unwrap();
    assert_eq!(held_again["resolved"], false);
    assert_eq!(held_again["held"], true);
    assert_eq!(
        held_again["replay"], false,
        "a held decision is not a terminal record, so this is a fresh attempt, not a replay"
    );

    // Past the rate window: the SAME violation id is still fully actionable
    // — a held decision was never permanently poisoned as "resolved".
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let resumed = decide(&mut client, repo, &id_b, Some("orch-1"), Some(generation))
        .await
        .unwrap();
    assert_eq!(resumed["resolved"], true);
    assert_eq!(resumed["held"], false);
    let ticket_b_resolved = get_ticket(&mut client, "TKT-ORCH-B").await;
    assert_eq!(ticket_b_resolved["payload"]["status"], "open");
    let resolved_updated_at = ticket_b_resolved["payload"]["updated_at"].clone();

    // NOW it is resolved and self-cleared: `terminal-assignee-active-work`
    // stops being raised once the ticket is back to `open`. But the durable
    // decision journal is consulted BEFORE a fresh report is ever built, so
    // a further decide call for the SAME id still returns the same terminal
    // record as `replay: true` — not "not found" — and never calls
    // `Tickets::reopen_if_in_progress` again.
    let replay = decide(&mut client, repo, &id_b, Some("orch-1"), Some(generation))
        .await
        .unwrap();
    assert_eq!(replay["resolved"], true);
    assert_eq!(replay["replay"], true);
    let ticket_b_after_replay = get_ticket(&mut client, "TKT-ORCH-B").await;
    assert_eq!(
        ticket_b_after_replay["payload"]["updated_at"], resolved_updated_at,
        "replaying a resolved orchestrator decision must not mutate the ticket again"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// ---------------------------------------------------------------------
// A resumed decide call for a violation whose data already changed
// out-of-band (modelling a crash between a prior attempt's mutation and
// its own journal write) converges safely with no further mutation.
// ---------------------------------------------------------------------

#[tokio::test]
async fn a_resumed_decide_after_out_of_band_mutation_does_not_repeat_it() {
    let home = tempfile::tempdir().unwrap();
    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;
    let repo = "crash-repo";

    write_ticket(
        &mut client,
        repo,
        "TKT-CRASH-1",
        ticket_payload(
            "in_progress",
            delivery("abc123", "rat/x/tkt-crash-1", "main"),
        ),
    )
    .await;

    let item = attention_next(&mut client, repo).await.unwrap();
    let item_id = item["id"].as_str().unwrap().to_string();

    // Model a PRIOR, crashed generation: its mechanical repair applied (the
    // ticket really did close) but it died before ever writing a decision
    // record — an update through the ordinary ticket API, entirely outside
    // `attention.decide`, with NO journal entry for this violation.
    client
        .call(
            "ticket.update",
            json!({"id": "TKT-CRASH-1", "status": "closed"}),
        )
        .await
        .unwrap();
    assert!(decision_events(&mut client, repo, &item_id)
        .await
        .is_empty());

    // A resumed caller, holding the id it captured before the "crash",
    // tries to finish the job. The report no longer contains this
    // violation at all (the ticket's own status already proves it), so this
    // must converge safely — no error, no re-mutation, no journal entry
    // fabricating a decision that was never actually made by this arm.
    let result = decide(&mut client, repo, &item_id, None, None)
        .await
        .unwrap();
    assert_eq!(result["resolved"], false);
    assert_eq!(
        result["reason"],
        "attention item not found: already resolved, or state has changed since it was surfaced",
    );

    let ticket = get_ticket(&mut client, "TKT-CRASH-1").await;
    assert_eq!(ticket["payload"]["status"], "closed");
    assert!(
        decision_events(&mut client, repo, &item_id)
            .await
            .is_empty(),
        "a resumed decide over stale, already-changed state must not fabricate a decision record"
    );
}

// ---------------------------------------------------------------------
// A REAL declared config.toml — not PolicyConfig::default() in Rust —
// governs the RPC boundary.
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_policy_file_governs_activation_not_just_defaults() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let config_path = home.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"
[policy]
orchestrator_action_allowlist = ["terminal-assignee-active-work"]
[policy.authority_overrides]
delivered-but-open = "human"
"#,
    )
    .unwrap();
    let mut config = Config::load(&config_path).unwrap();
    config.harness.default = "fake".into();
    // Sanity: the declared file, not `PolicyConfig::default()`.
    assert_ne!(config.policy, PolicyConfig::default());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new(layout.clone(), &config).unwrap();
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo = repo_name_of(repo_dir.path());
    client
        .call(
            "repo.add",
            json!({"name": &repo, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // The narrowing override is active: this build's own default for
    // `delivered-but-open` is Mechanical, but the FILE says Human.
    write_ticket(
        &mut client,
        &repo,
        "TKT-DECLARED-1",
        ticket_payload(
            "in_progress",
            delivery("abc123", "rat/x/tkt-declared-1", "main"),
        ),
    )
    .await;
    let overridden = attention_next(&mut client, &repo)
        .await
        .expect("delivered-but-open must still surface as an attention item");
    assert_eq!(overridden["kind"], "delivered-but-open");
    assert_eq!(
        overridden["authority"], "mechanical",
        "the code's own built-in classification is unchanged"
    );
    assert_eq!(
        overridden["effective_authority"], "human",
        "the DECLARED FILE's narrowing override must be what is active"
    );
    let before = get_ticket(&mut client, "TKT-DECLARED-1").await;
    // No human-gate template is registered for this kind, so this refuses
    // outright rather than inventing gate text — but critically, it does
    // NOT auto-close the ticket the way the code's own default would.
    assert!(decide(
        &mut client,
        &repo,
        overridden["id"].as_str().unwrap(),
        None,
        None,
    )
    .await
    .is_err());
    let after = get_ticket(&mut client, "TKT-DECLARED-1").await;
    assert_eq!(
        before, after,
        "the narrowed-to-human item must not auto-close"
    );

    // The allowlist entry is active: an orchestrator holding a live lease
    // CAN resolve exactly the kind this file named.
    let owner = spawn_completed_agent(&mut client, repo_dir.path(), "declared-owner").await;
    write_ticket(
        &mut client,
        &repo,
        "TKT-DECLARED-2",
        ticket_payload("in_progress", json!({"assignee": owner})),
    )
    .await;
    // `attention.next`'s single shared cursor would still be sitting on
    // TKT-DECLARED-1 (a human item that never advances anything), so this
    // reads the violation directly rather than through `attention.next`.
    let allowlisted = reconcile_violation(
        &mut client,
        &repo,
        "terminal-assignee-active-work",
        "TKT-DECLARED-2",
    )
    .await;
    let lease = client
        .call("lease.acquire", json!({"repo": &repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let result = decide(
        &mut client,
        &repo,
        allowlisted["id"].as_str().unwrap(),
        Some("orch-1"),
        Some(lease["generation"].as_u64().unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(result["resolved"], true);
    let ticket = get_ticket(&mut client, "TKT-DECLARED-2").await;
    assert_eq!(ticket["payload"]["status"], "open");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// ---------------------------------------------------------------------
// Lease: resumes for the same holder, blocks a different live holder,
// replaces only once expired (bumping generation, fencing the old
// holder), and survives a genuine daemon restart.
// ---------------------------------------------------------------------

#[tokio::test]
async fn lease_resumes_for_the_same_holder_and_blocks_a_different_live_holder() {
    let home = tempfile::tempdir().unwrap();
    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;
    let repo = "lease-repo";

    let first = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(first["generation"], 1);

    // A "disconnect": the same holder simply calls acquire again.
    let resumed = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(resumed["generation"], 1);
    assert_eq!(resumed["holder"], "orch-1");

    // A different holder cannot preempt a live lease.
    let err = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-2"}))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("orch-1"), "{err}");
}

#[tokio::test]
async fn a_different_holder_replaces_an_expired_lease_and_the_old_holder_is_fenced() {
    let home = tempfile::tempdir().unwrap();
    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;
    let repo = "lease-expiry-repo";

    let first = client
        .call(
            "lease.acquire",
            json!({"repo": repo, "holder": "orch-1", "ttl_secs": 1}),
        )
        .await
        .unwrap();
    let generation = first["generation"].as_u64().unwrap();

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let second = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-2"}))
        .await
        .unwrap();
    assert_eq!(second["holder"], "orch-2");
    assert_eq!(second["generation"].as_u64().unwrap(), generation + 1);

    // The superseded holder is fenced out of both renew and (transitively,
    // through the same fence check) any further decision.
    let err = client
        .call(
            "lease.renew",
            json!({"repo": repo, "holder": "orch-1", "generation": generation}),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("fencing failed"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_and_cursor_survive_a_genuine_daemon_restart() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let config = Config {
        policy: allow(&["terminal-assignee-active-work"]),
        ..Config::default()
    };
    let repo = repo_name_of(repo_dir.path());
    let repo = repo.as_str();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(QUICK_DONE));

    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": repo, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let owner = spawn_completed_agent(&mut client, repo_dir.path(), "orch-restart").await;
    write_ticket(
        &mut client,
        repo,
        "TKT-RESTART-1",
        ticket_payload("in_progress", json!({"assignee": owner})),
    )
    .await;

    let acquired = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    let generation = acquired["generation"].as_u64().unwrap();

    // Actually ADVANCE the cursor through a real orchestrator decision — a
    // bare acquire/renew never touches the cursor at all, so a restart test
    // that stopped there would prove nothing about CURSOR survival
    // specifically, only the lease's own generation/holder fields.
    let item = attention_next(&mut client, repo)
        .await
        .expect("terminal-assignee-active-work must surface");
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

    // Genuinely kill and replace the daemon process (same pattern as
    // `live_landing_restart.rs`), not merely drop-and-recreate an in-memory
    // Space — the lease store's crash-safety claim is specifically about
    // surviving that.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The SAME holder resumes with its generation AND cursor untouched.
    let resumed = client
        .call("lease.acquire", json!({"repo": repo, "holder": "orch-1"}))
        .await
        .unwrap();
    assert_eq!(resumed["generation"].as_u64().unwrap(), generation);
    assert_eq!(resumed["cursor"].as_str().unwrap(), cursor);

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
    handle_b.abort();
    let _ = handle_b.await;
}

// ---------------------------------------------------------------------
// No RPC method exists that could widen an orchestrator session's own
// authority — the only route is a human editing config.toml and
// restarting the daemon.
// ---------------------------------------------------------------------

#[tokio::test]
async fn no_rpc_method_exists_to_widen_orchestrator_authority() {
    let home = tempfile::tempdir().unwrap();
    let mut client = daemon_with_policy(home.path(), PolicyConfig::default()).await;

    for method in [
        "policy.update",
        "policy.set",
        "authority.set",
        "authority.override",
        "config.set",
    ] {
        let err = client
            .call(method, json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unknown method"),
            "expected {method} to not exist as an RPC method: {err}"
        );
    }
}
