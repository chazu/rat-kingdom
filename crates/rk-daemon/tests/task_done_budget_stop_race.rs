//! TKT-01M0J5KT4TCH03W48MR9T7EJ27 (2026-08-21 Cinder-11 incident): a rat's
//! `rk done` durably writes a `task_done` tuple, but the harness's OWN turn-
//! completion event is delivered asynchronously — if a concurrent budget
//! hard-stop kills the process first, that event can simply never arrive,
//! and (for a short/one-turn generation with no earlier withheld turn) the
//! completion and its landing trigger were silently lost. Fixed by
//! `Supervisor::reconcile_task_done` (`crates/rk-daemon/src/supervisor.rs`):
//! an event-feed + interval background loop that reacts to the durable
//! `task_done` tuple directly, CASing the record to `Completed` under the
//! SAME registry lock `enforce_budget`'s hard-stop CAS uses.
//!
//! Three properties, three tests:
//!
//!  - [`task_done_wins_and_a_later_budget_check_cannot_overwrite_it`]: a
//!    `task_done` that lands while the record is still live completes it,
//!    and a budget check that runs afterward is a no-op against the
//!    already-`Completed` record (`enforce_budget`'s own `is_live()` guard).
//!  - [`budget_stop_wins_durably_first_and_a_late_task_done_is_fenced`]: a
//!    budget hard-stop that durably wins first leaves the record `Stopped`;
//!    a `task_done` that arrives afterward is retained as evidence with an
//!    explicit recovery action, never mutates the terminal state, and both
//!    the evidence write and the (never-fired) completion are idempotent
//!    under a repeat reconcile pass.
//!  - [`restart_mid_reconcile_barrier_still_completes_the_generation`]: a
//!    genuinely two-`Daemon`-process restart (same shape as
//!    `live_landing_restart.rs`), landing the kill exactly between the
//!    reconcile pass claiming the publish right and the registry CAS itself
//!    (`crate::fault::barrier`'s `task-done-pre-route`), still converges to
//!    exactly one `Completed` + one `harness_result` on the successor daemon.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// `RK_FAKE_HARNESS_CMD` is process-global; tests in this binary that touch
/// it must serialize (same discipline as `agent_lifecycle.rs`'s
/// `HARNESS_ENV_LOCK`).
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("f"), "x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn tiny_sweep_config() -> rk_core::config::SupervisorConfig {
    // Fast fallback tick for `reconcile_task_done`'s interval leg; the
    // liveness/burn sweep itself is irrelevant here and left off so it
    // cannot interfere with a `Stopped` record's bookkeeping.
    rk_core::config::SupervisorConfig {
        enabled: false,
        interval_secs: 1,
        ..Default::default()
    }
}

async fn agent_state(client: &mut Client, name: &str) -> String {
    client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap()["agent"]["state"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

async fn wait_for_state(client: &mut Client, name: &str, want: &[&str]) -> String {
    let mut last = String::new();
    for _ in 0..300 {
        last = agent_state(client, name).await;
        if want.contains(&last.as_str()) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agent {name} never reached one of {want:?}, last seen: {last}");
}

async fn harness_result_events(client: &mut Client, repo: &str, agent: &str) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo, "identity": "harness_result"}),
        )
        .await
        .unwrap();
    res["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["payload"]["agent"] == agent)
        .collect()
}

async fn late_evidence_artifacts(client: &mut Client, repo: &str, agent: &str) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "artifact", "scope": repo, "identity": "late_task_done_evidence"}),
        )
        .await
        .unwrap();
    res["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["payload"]["agent"] == agent)
        .collect()
}

/// Declares done then goes quiet without ever printing a final `result`
/// line — the harness's own `Completed` event never arrives, so the ONLY
/// way this generation can ever complete is `reconcile_task_done` reacting
/// to the durable `task_done` tuple directly. Matches the "multi-turn"
/// shape `mid_flight_result.rs` uses for the same reason (a real Claude
/// session stays alive between turns).
const DECLARES_THEN_GOES_QUIET: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"race-1"}'
rk_done "finished the work"
sleep 300
"#;

/// Bursts far past a tiny budget cap in one `assistant` usage message, waits
/// long enough for the daemon's (synchronous, in-process) `enforce_budget`
/// CAS and its kill dispatch to have landed, and only THEN calls `rk_done`
/// — reproducing the narrow real-world window where an `rk done` RPC that
/// was already in flight (or, as here, deliberately delayed to land after
/// the stop) completes despite the SIGTERM already having been sent. Ignores
/// TERM (rather than trapping-and-exiting the way a real harness winding
/// down would) so the script survives the budget hard-stop's SIGTERM long
/// enough to still make that RPC call — a signal delivered to the process
/// group at kill time never reaches the `rk_done` child forked afterward.
const BUDGET_WINS_THEN_LATE_DONE: &str = r#"
trap '' TERM
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"race-2"}'
echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"burning tokens"}],"usage":{"input_tokens":500000,"output_tokens":100000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
sleep 0.5
rk_done "finished despite the stop"
sleep 300
"#;

#[tokio::test]
async fn task_done_wins_and_a_later_budget_check_cannot_overwrite_it() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(DECLARES_THEN_GOES_QUIET),
    );
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    // Generous cap: this generation must complete on its `task_done` alone,
    // long before any budget concern is even close.
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget {
            max_usd: 1000.0,
            max_tokens: 0,
            warn_at: 900.0,
        },
        space,
    )
    .unwrap();
    daemon.set_sweep_config(tiny_sweep_config());
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    let ticket = client
        .call("ticket.new", json!({"title": "race winner", "scope": &repo_name}))
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": &ticket_id,
                "harness": "fake",
                "model": "haiku",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // The harness never emits its own `Completed` event (see
    // `DECLARES_THEN_GOES_QUIET`) — reaching `completed` here is proof
    // `reconcile_task_done` is what drove the transition.
    let state = wait_for_state(&mut client, &name, &["completed", "failed", "stopped"]).await;
    assert_eq!(
        state, "completed",
        "a durably-accepted task_done must complete the generation on its own"
    );

    let events = harness_result_events(&mut client, &repo_name, &name).await;
    assert_eq!(
        events.len(),
        1,
        "exactly one landing candidate: exactly one harness_result for this generation"
    );
    assert_eq!(events[0]["payload"]["declared_done"], true);
    assert_eq!(events[0]["payload"]["is_error"], false);

    // Delivery-mode behavior survives the reconcile-driven path exactly like
    // the harness-event path: a merge-mode ticket does not close on a clean
    // completion alone, only once its branch actually lands.
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_ne!(
        t["ticket"]["payload"]["status"], "closed",
        "a clean reconcile-driven completion must not close a merge-mode ticket by itself"
    );

    // A budget check that runs AFTER the completion (simulated directly via
    // another Usage event that would trip Stop on a live record) must be a
    // no-op against the already-Completed record — `enforce_budget`'s own
    // `is_live()` guard, unchanged by this fix, is what a "concurrent or
    // later" sweep relies on.
    client
        .call(
            "space.out",
            json!({
                "category": "event",
                "identity": "harness_result",
                "scope": &repo_name,
                "payload": {"noop": true}
            }),
        )
        .await
        .ok(); // best-effort nudge to wake any listeners; irrelevant if refused
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        agent_state(&mut client, &name).await,
        "completed",
        "state must remain Completed"
    );
    let events_after = harness_result_events(&mut client, &repo_name, &name).await;
    assert_eq!(
        events_after.len(),
        1,
        "a later sweep tick must not publish a second completion (idempotent)"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn budget_stop_wins_durably_first_and_a_late_task_done_is_fenced() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(BUDGET_WINS_THEN_LATE_DONE),
    );
    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget {
            max_usd: 0.5,
            max_tokens: 0,
            warn_at: 0.1,
        },
        space,
    )
    .unwrap();
    daemon.set_sweep_config(tiny_sweep_config());
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    let ticket = client
        .call("ticket.new", json!({"title": "race loser", "scope": &repo_name}))
        .await
        .unwrap();
    let ticket_id = ticket["ticket"]["identity"].as_str().unwrap().to_string();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": &ticket_id,
                "harness": "fake",
                "model": "haiku",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // The budget-cap burst drives this to `stopped` well before the delayed
    // `rk_done` call even runs.
    let state = wait_for_state(&mut client, &name, &["stopped", "completed", "failed"]).await;
    assert_eq!(
        state, "stopped",
        "the budget hard-stop must win durably first"
    );

    // The late `task_done` must be retained as evidence, not silently
    // dropped and not allowed to flip the terminal state back to Completed.
    let mut evidence = Vec::new();
    for _ in 0..100 {
        evidence = late_evidence_artifacts(&mut client, &repo_name, &name).await;
        if !evidence.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        evidence.len(),
        1,
        "the late task_done must be retained as durable evidence exactly once"
    );
    let recovery = evidence[0]["payload"]["recovery_action"]
        .as_str()
        .unwrap_or("");
    assert!(
        recovery.contains("rk land") || recovery.contains("rk respawn"),
        "evidence must carry an explicit recovery action, got: {recovery}"
    );
    assert_eq!(evidence[0]["payload"]["terminal_state"], "Stopped");

    // The terminal state itself must never have been mutated back.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        agent_state(&mut client, &name).await,
        "stopped",
        "a fenced late task_done must never mutate the terminal state"
    );
    assert!(
        harness_result_events(&mut client, &repo_name, &name)
            .await
            .is_empty(),
        "a fenced generation must never publish a completion event or landing trigger"
    );

    // No premature ticket close either — the fenced path never touches the
    // ticket at all.
    let t = client
        .call("ticket.get", json!({"id": &ticket_id}))
        .await
        .unwrap();
    assert_ne!(t["ticket"]["payload"]["status"], "closed");

    // Idempotence: further reconcile ticks (interval + event-feed wakeups
    // from the polling above) must not duplicate the evidence artifact.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let evidence_again = late_evidence_artifacts(&mut client, &repo_name, &name).await;
    assert_eq!(
        evidence_again.len(),
        1,
        "repeat reconcile passes must not duplicate the evidence artifact"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_mid_reconcile_barrier_still_completes_the_generation() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    // Arm the barrier BEFORE daemon A starts: `reconcile_task_done` parks
    // just before its registry CAS the first time it reaches that point for
    // this generation (`crate::fault::barrier`, gated to `debug_assertions`
    // — see that module's doc comment for why a barrier and not a sleep).
    std::fs::write(home.path().join("fault-barrier"), "task-done-pre-route").unwrap();

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(DECLARES_THEN_GOES_QUIET),
    );
    let config = {
        let mut c = rk_core::config::Config::default();
        c.harness.default = "fake".into();
        c.supervisor.enabled = false;
        c.supervisor.interval_secs = 1;
        c
    };

    // Daemon A: a genuine on-disk `Space` (`Daemon::new`), same shape as
    // `live_landing_restart.rs` — a second `Daemon::new` below must inherit
    // this one's durable state, not start from an empty store.
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "restart race",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for the barrier to be reached: proof the reconcile pass already
    // found the task_done tuple, claimed the publish right, and is parked
    // exactly before the registry CAS.
    let reached = home.path().join("fault-barrier.reached");
    let mut hit = false;
    for _ in 0..200 {
        if reached.exists() {
            hit = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(hit, "reconcile_task_done never reached the pre-route barrier");

    // The record must still be live — the CAS has not landed yet.
    assert_eq!(
        agent_state(&mut client, &name).await,
        "running",
        "the barrier must land strictly before the Completed CAS"
    );

    // The kill: abort the daemon's task outright (same rationale as
    // `live_landing_restart.rs`) so the parked reconcile future is
    // genuinely cut off, not allowed to finish first.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    // Disarm before the successor starts — a second daemon must come up
    // disarmed (`crate::fault`'s documented arming contract), or it would
    // park on the same barrier forever too.
    std::fs::remove_file(home.path().join("fault-barrier")).ok();
    std::fs::remove_file(&reached).ok();

    // Daemon B: fresh `Daemon::new` over the SAME on-disk home. The record
    // survived the kill as `Orphaned` (daemon restart semantics) with its
    // `task_done` still durable; `reconcile_task_done` must pick it back up
    // on its own, with no operator intervention.
    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    let state = wait_for_state(&mut client, &name, &["completed", "failed", "stopped"]).await;
    assert_eq!(
        state, "completed",
        "the generation must converge to Completed on the successor daemon"
    );
    let events = harness_result_events(&mut client, &repo_name, &name).await;
    assert_eq!(
        events.len(),
        1,
        "exactly one harness_result must be published across the restart, not zero and not two"
    );

    handle_b.abort();
    let _ = handle_b.await;
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
