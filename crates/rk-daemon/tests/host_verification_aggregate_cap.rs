//! P3.1 (TKT-vilug-hujok-bolis): the real native CLI/RPC journey for the
//! optional aggregate host-wide verification admission cap
//! (`[policy] verification_admission_aggregate_limit`), layered above each
//! repository's own existing `VerificationAdmission` bound.
//!
//! Two independently registered fixture repos, bounded controlled named
//! checks (a shell one-liner that writes its own pid, then blocks on an
//! explicit release file the test controls — never a fixed sleep as the
//! success criterion). Every concurrency/serialization assertion below is
//! driven by polling the daemon's own native `status` RPC
//! (`verification_host` / `capacity[repo].verification`) until a real
//! condition holds, bounded by a deadline — not a fixed sleep-and-hope.
//!
//! Scenarios (closest existing templates: `verification_saturation_regression.rs`
//! for the marker/peak-concurrency technique, `managed_verification_cancel_e2e.rs`
//! for pid-file barriers and RPC-disconnect cancellation):
//! - `cap1`: aggregate limit 1 serializes two otherwise-unbounded repos.
//! - `cap2`: aggregate limit 2 admits two concurrently.
//! - a repo's own per-repo cap still holds under a more generous aggregate,
//!   and a request queued behind ITS OWN saturated repo is never counted by
//!   the aggregate's `waiting` count (it hasn't reached that semaphore yet).
//! - aggregate limit 0 (disabled, the default) preserves prior behavior.
//! - cancelling a queued request and an executing request: the owned
//!   child/process tree is reaped, no permit leaks, a waiting peer
//!   progresses, and read-only status keeps responding throughout.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

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

fn init_repo(dir: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
    dir.file_name().unwrap().to_string_lossy().to_string()
}

/// One barrier-controlled check body: writes its own pid to
/// `<shared>/<marker>.pid` the instant it starts (proving it genuinely
/// executed, not merely was admitted), then blocks — polling on a short
/// fixed interval, never sleeping past a bounded budget — until the test
/// deposits `<shared>/<marker>.release`, then exits 0. `marker` is a
/// caller-chosen globally-unique name (NOT necessarily the check's own
/// name): two repos each registering a check literally named "go" must
/// still write to two distinct marker files in one shared directory.
fn barrier_check_body(shared: &Path, marker: &str) -> String {
    let shared = shared.display();
    format!(
        r#"echo $$ > "{shared}/{marker}.pid"; for i in $(seq 1 600); do [ -f "{shared}/{marker}.release" ] && exit 0; sleep 0.05; done; echo "barrier {marker} never released" 1>&2; exit 9"#
    )
}

fn cue_command(body: &str) -> String {
    body.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Installs one named check per `(check_name, marker, timeout)` triple in
/// `checks`, each running [`barrier_check_body`] under its own marker and
/// its own declared timeout — the timeout doubles as the check's own
/// admission-wait budget (`verify_repo_check` always passes
/// `admission_timeout: None`, so `ManagedVerification::run` falls back to
/// the check's own timeout), letting a test give one check a short,
/// deterministic admission-wait window distinct from its siblings'.
fn write_checks_with_timeouts(repo: &Path, shared: &Path, checks: &[(&str, &str, &str)]) {
    let mut cue = String::from("checks: [\n");
    for (name, marker, timeout) in checks {
        let body = barrier_check_body(shared, marker);
        cue.push_str(&format!(
            "    {{name: \"{name}\", command: \"{}\", timeout: \"{timeout}\", environmentPolicy: \"strip_rk_spawn\"}},\n",
            cue_command(&body)
        ));
    }
    cue.push_str("]\n");
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), cue).unwrap();
}

/// Installs one named check per `(check_name, marker)` pair, all under a
/// generous shared 30s timeout — the common case, for a test with no need
/// for a per-check admission-wait budget of its own.
fn write_barrier_checks(repo: &Path, shared: &Path, checks: &[(&str, &str)]) {
    let with_timeouts: Vec<(&str, &str, &str)> = checks
        .iter()
        .map(|(name, marker)| (*name, *marker, "30s"))
        .collect();
    write_checks_with_timeouts(repo, shared, &with_timeouts);
}

fn release(shared: &Path, marker: &str) {
    std::fs::write(shared.join(format!("{marker}.release")), b"go").unwrap();
}

fn pid_path(shared: &Path, marker: &str) -> std::path::PathBuf {
    shared.join(format!("{marker}.pid"))
}

const POLL_DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(30);

/// Poll `path` until it exists (a real check process genuinely started),
/// bounded by [`POLL_DEADLINE`] — never a fixed sleep as the success
/// criterion.
async fn wait_for_start(path: &Path) {
    let deadline = tokio::time::Instant::now() + POLL_DEADLINE;
    loop {
        if path.exists() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "check never started (no pid file at {}) within {POLL_DEADLINE:?}",
            path.display()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// A single point-in-time check, used only ALONGSIDE a status-RPC condition
/// that already proves the request is genuinely queued — never as the sole
/// proof of serialization.
fn assert_not_started_yet(path: &Path, context: &str) {
    assert!(
        !path.exists(),
        "{context}: {} must not exist yet — the request should still be queued, not executing",
        path.display()
    );
}

/// Bound on a single `status` RPC round-trip — independent of, and much
/// smaller than, [`POLL_DEADLINE`]'s overall condition budget. Without this,
/// a wedged daemon connection would hang this `.await` forever: the
/// deadline check in [`poll_status_until`] below only runs AFTER an await
/// returns, so it can never bound an RPC call that itself never returns.
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(5);

async fn status(client: &mut Client) -> Value {
    tokio::time::timeout(RPC_CALL_TIMEOUT, client.call("status", json!({})))
        .await
        .expect("status RPC must respond within its own bounded timeout, not hang indefinitely")
        .unwrap()
}

/// Poll `status` until `pred` holds, bounded by [`POLL_DEADLINE`] overall —
/// AND by [`RPC_CALL_TIMEOUT`] on every individual `status` call, so a
/// single stuck round-trip cannot silently turn this into an unbounded
/// hang. Returns the passing status for further inspection.
async fn poll_status_until(
    client: &mut Client,
    description: &str,
    mut pred: impl FnMut(&Value) -> bool,
) -> Value {
    let deadline = tokio::time::Instant::now() + POLL_DEADLINE;
    loop {
        let s = status(client).await;
        if pred(&s) {
            return s;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition never became true within {POLL_DEADLINE:?}: {description}; last status: {s}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn host_executing(s: &Value) -> u64 {
    s["verification_host"]["executing"].as_u64().unwrap_or(0)
}

fn host_waiting(s: &Value) -> u64 {
    s["verification_host"]["waiting"].as_u64().unwrap_or(0)
}

fn repo_verify_in_flight(s: &Value, repo: &str) -> u64 {
    s["capacity"][repo]["verification"]["in_flight"]
        .as_u64()
        .unwrap_or(0)
}

async fn spawn_daemon_with_limits(
    layout: &Layout,
    aggregate_limit: u32,
    repo_limits: HashMap<String, u32>,
) {
    let space = Space::open(&layout.db_path()).unwrap();
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_verification_admission_limits(0, repo_limits);
    daemon.set_verification_admission_aggregate_limit(aggregate_limit);
    tokio::spawn(daemon.run());
}

/// Fires `verify.run` for `(repo, check)` over its OWN fresh connection —
/// genuine concurrency needs one connection per in-flight request.
fn spawn_verify(layout: Layout, repo: String, check: String) -> tokio::task::JoinHandle<Value> {
    tokio::spawn(async move {
        let mut client = Client::connect_as_operator(&layout).await.unwrap();
        client
            .call("verify.run", json!({"repo": repo, "check": check}))
            .await
            .unwrap_or_else(|e| panic!("verify.run({repo}, {check}) failed: {e}"))
    })
}

async fn register(client: &mut Client, name: &str, dir: &Path) {
    client
        .call(
            "repo.add",
            json!({"name": name, "path": dir.to_string_lossy()}),
        )
        .await
        .unwrap();
}

/// A prepared, but not-yet-registered, fixture repo: created before the
/// daemon starts so its NAME is known up front, for a test that needs to
/// hand a per-repo admission override to `spawn_daemon_with_limits` keyed by
/// that exact name.
struct PreparedRepo {
    dir: tempfile::TempDir,
    name: String,
}

fn prepare_repo(shared: &Path, checks: &[(&str, &str)]) -> PreparedRepo {
    let dir = tempfile::tempdir().unwrap();
    let name = init_repo(dir.path());
    write_barrier_checks(dir.path(), shared, checks);
    PreparedRepo { dir, name }
}

/// aggregate limit 1 serializes two otherwise-unbounded repos: the second
/// request must not start until the first releases, proven via the native
/// status RPC's `executing`/`waiting` counts (a real condition, polled to a
/// deadline), not by racing on timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn aggregate_cap_1_serializes_two_repos() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let a = prepare_repo(shared.path(), &[("go", "a")]);
    let b = prepare_repo(shared.path(), &[("go", "b")]);

    spawn_daemon_with_limits(&layout, 1, HashMap::new()).await;
    let mut client = connect(&layout).await;
    register(&mut client, &a.name, a.dir.path()).await;
    register(&mut client, &b.name, b.dir.path()).await;

    let a_call = spawn_verify(layout.clone(), a.name.clone(), "go".into());
    wait_for_start(&pid_path(shared.path(), "a")).await;
    poll_status_until(&mut client, "A occupies the one aggregate permit", |s| {
        host_executing(s) == 1
    })
    .await;

    let b_call = spawn_verify(layout.clone(), b.name.clone(), "go".into());
    poll_status_until(
        &mut client,
        "B is genuinely queued behind the saturated aggregate cap",
        |s| host_executing(s) == 1 && host_waiting(s) == 1,
    )
    .await;
    assert_not_started_yet(
        &pid_path(shared.path(), "b"),
        "aggregate cap 1 must serialize B behind A",
    );

    release(shared.path(), "a");
    let a_result = a_call.await.unwrap();
    assert_eq!(a_result["exit"], json!(0), "{a_result:#?}");

    wait_for_start(&pid_path(shared.path(), "b")).await;
    poll_status_until(
        &mut client,
        "B now occupies the freed aggregate permit",
        |s| host_executing(s) == 1 && host_waiting(s) == 0,
    )
    .await;

    release(shared.path(), "b");
    let b_result = b_call.await.unwrap();
    assert_eq!(b_result["exit"], json!(0), "{b_result:#?}");

    let final_status = poll_status_until(&mut client, "capacity fully drains", |s| {
        host_executing(s) == 0 && host_waiting(s) == 0
    })
    .await;
    assert_eq!(final_status["verification_host"]["limit"], json!(1));
}

/// aggregate limit 2 admits both concurrently — both must be genuinely
/// executing (real pid files, real `executing == 2`) before either releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn aggregate_cap_2_allows_two_concurrent_checks() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let a = prepare_repo(shared.path(), &[("go", "a")]);
    let b = prepare_repo(shared.path(), &[("go", "b")]);

    spawn_daemon_with_limits(&layout, 2, HashMap::new()).await;
    let mut client = connect(&layout).await;
    register(&mut client, &a.name, a.dir.path()).await;
    register(&mut client, &b.name, b.dir.path()).await;

    let a_call = spawn_verify(layout.clone(), a.name.clone(), "go".into());
    let b_call = spawn_verify(layout.clone(), b.name.clone(), "go".into());

    wait_for_start(&pid_path(shared.path(), "a")).await;
    wait_for_start(&pid_path(shared.path(), "b")).await;
    poll_status_until(
        &mut client,
        "both A and B genuinely execute concurrently under aggregate=2",
        |s| host_executing(s) == 2 && host_waiting(s) == 0,
    )
    .await;

    release(shared.path(), "a");
    release(shared.path(), "b");
    assert_eq!(a_call.await.unwrap()["exit"], json!(0));
    assert_eq!(b_call.await.unwrap()["exit"], json!(0));

    poll_status_until(&mut client, "capacity fully drains", |s| {
        host_executing(s) == 0
    })
    .await;
}

/// A repository's own per-repo `verification_admission_limit_by_repo` still
/// holds even under a much more generous aggregate cap. Proven two ways,
/// deliberately NOT by inferring anything from elapsed wall-clock time:
///
/// 1. `go2` declares its OWN short (1s) timeout — the check's own timeout
///    doubles as its admission-wait budget (`write_checks_with_timeouts`),
///    so while `go1` still holds repo A's one permit, `go2` is GUARANTEED
///    to exhaust that budget and report back a genuine, daemon-produced
///    `"verdict": "infra"` / `"reason": "admission-timeout"` outcome — not
///    an inference from "it hasn't started yet after N polls", which
///    cannot distinguish "genuinely blocked on admission" from "merely
///    slow to reach the daemon over the wire".
/// 2. An ELIGIBLE DIFFERENT repo (B, well under the generous aggregate=5
///    cap) is fired and proven to genuinely execute CONCURRENTLY while
///    `go2` is still blocked on repo A's own saturated lane — the actual
///    cross-repo head-of-line-blocking property `ManagedVerification::run`'s
///    acquire order exists to prevent. A test that registers only one repo
///    cannot exercise this at all.
///
/// Also confirms `go2` never reaches (and so is never counted by) the
/// aggregate host semaphore — architecturally guaranteed, since its own
/// admission-timeout firing at the per-repo layer means the sequential
/// acquire chain in `ManagedVerification::run` never gets far enough to
/// even attempt the host semaphore.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn repo_specific_cap_still_holds_under_a_higher_aggregate_cap() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let a_dir = tempfile::tempdir().unwrap();
    let a_name = init_repo(a_dir.path());
    write_checks_with_timeouts(
        a_dir.path(),
        shared.path(),
        &[("go1", "a1", "30s"), ("go2", "a2", "1s")],
    );
    let b = prepare_repo(shared.path(), &[("go", "b")]);

    spawn_daemon_with_limits(&layout, 5, HashMap::from([(a_name.clone(), 1)])).await;
    let mut client = connect(&layout).await;
    register(&mut client, &a_name, a_dir.path()).await;
    register(&mut client, &b.name, b.dir.path()).await;

    let call1 = spawn_verify(layout.clone(), a_name.clone(), "go1".into());
    wait_for_start(&pid_path(shared.path(), "a1")).await;
    poll_status_until(
        &mut client,
        "check1 occupies repo A's own WIP=1 permit and the aggregate permit",
        |s| repo_verify_in_flight(s, &a_name) == 1 && host_executing(s) == 1,
    )
    .await;

    // go2 genuinely reaches the daemon and blocks on repo A's own saturated
    // per-repo semaphore; its 1s admission-wait budget will deterministically
    // elapse while go1 is still held.
    let call2 = spawn_verify(layout.clone(), a_name.clone(), "go2".into());

    // While go2 is genuinely blocked, repo B — an eligible different repo —
    // must progress concurrently: exactly the cross-repo
    // head-of-line-blocking property under test.
    let call_b = spawn_verify(layout.clone(), b.name.clone(), "go".into());
    wait_for_start(&pid_path(shared.path(), "b")).await;
    poll_status_until(
        &mut client,
        "repo B genuinely executes concurrently while repo A's own lane is saturated",
        |s| host_executing(s) == 2,
    )
    .await;
    assert_not_started_yet(
        &pid_path(shared.path(), "a2"),
        "repo A's own WIP=1 cap must still serialize go2 behind go1",
    );

    let call2_result = tokio::time::timeout(Duration::from_secs(10), call2)
        .await
        .expect("go2 must settle once its own 1s admission-wait budget elapses")
        .unwrap();
    assert_eq!(
        call2_result["verdict"],
        json!("infra"),
        "go2, blocked behind go1 on repo A's own saturated WIP=1 lane, must report a genuine \
         admission-timeout verdict, not run: {call2_result:#?}"
    );
    assert_eq!(
        call2_result["reason"],
        json!("admission-timeout"),
        "{call2_result:#?}"
    );
    assert!(
        !pid_path(shared.path(), "a2").exists(),
        "go2 must never have spawned a real process at all — it never left repo A's own \
         admission queue"
    );

    let after_call2 = status(&mut client).await;
    assert_eq!(
        host_waiting(&after_call2),
        0,
        "go2 timed out at the per-repo layer and so architecturally never reached the \
         aggregate semaphore — it must never appear in its waiting count: {after_call2}"
    );

    release(shared.path(), "b");
    assert_eq!(call_b.await.unwrap()["exit"], json!(0));

    release(shared.path(), "a1");
    assert_eq!(call1.await.unwrap()["exit"], json!(0));

    poll_status_until(&mut client, "capacity fully drains", |s| {
        host_executing(s) == 0 && repo_verify_in_flight(s, &a_name) == 0
    })
    .await;
}

/// aggregate limit 0 (disabled, the default) preserves prior behavior: two
/// otherwise-unbounded repos run genuinely concurrently, and
/// `verification_host` reports the disabled shape throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn aggregate_cap_disabled_preserves_prior_behavior() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let a = prepare_repo(shared.path(), &[("go", "a")]);
    let b = prepare_repo(shared.path(), &[("go", "b")]);

    spawn_daemon_with_limits(&layout, 0, HashMap::new()).await;
    let mut client = connect(&layout).await;
    register(&mut client, &a.name, a.dir.path()).await;
    register(&mut client, &b.name, b.dir.path()).await;

    let disabled = status(&mut client).await;
    assert_eq!(disabled["verification_host"]["limit"], json!(0));
    assert_eq!(disabled["verification_host"]["executing"], json!(0));
    assert_eq!(disabled["verification_host"]["waiting"], json!(0));

    let a_call = spawn_verify(layout.clone(), a.name.clone(), "go".into());
    let b_call = spawn_verify(layout.clone(), b.name.clone(), "go".into());
    // No aggregate serialization to poll for — the very absence of an
    // aggregate condition is the point. Both must start on their own.
    wait_for_start(&pid_path(shared.path(), "a")).await;
    wait_for_start(&pid_path(shared.path(), "b")).await;

    let while_running = status(&mut client).await;
    assert_eq!(
        while_running["verification_host"]["limit"],
        json!(0),
        "disabled must never report a nonzero limit while checks run: {while_running}"
    );
    assert_eq!(while_running["verification_host"]["executing"], json!(0));
    assert_eq!(while_running["verification_host"]["waiting"], json!(0));

    release(shared.path(), "a");
    release(shared.path(), "b");
    assert_eq!(a_call.await.unwrap()["exit"], json!(0));
    assert_eq!(b_call.await.unwrap()["exit"], json!(0));
}

/// Cancel a QUEUED aggregate request and an EXECUTING one: the owned
/// child/process tree is reaped, no permit leaks, a waiting peer progresses,
/// and read-only status keeps responding throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancelling_queued_and_executing_aggregate_requests_reaps_children_and_lets_a_peer_progress(
) {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let a = prepare_repo(shared.path(), &[("go", "a")]);
    let b = prepare_repo(shared.path(), &[("go1", "b1"), ("go2", "b2")]);

    spawn_daemon_with_limits(&layout, 1, HashMap::new()).await;
    let mut client = connect(&layout).await;
    register(&mut client, &a.name, a.dir.path()).await;
    register(&mut client, &b.name, b.dir.path()).await;

    // A: executing, holding the aggregate's one and only permit.
    let a_layout = layout.clone();
    let a_repo = a.name.clone();
    let a_conn = tokio::spawn(async move {
        let mut client = Client::connect_as_operator(&a_layout).await.unwrap();
        client
            .call("verify.run", json!({"repo": a_repo, "check": "go"}))
            .await
    });
    wait_for_start(&pid_path(shared.path(), "a")).await;
    poll_status_until(&mut client, "A occupies the one permit", |s| {
        host_executing(s) == 1
    })
    .await;
    let a_pid: i32 = std::fs::read_to_string(pid_path(shared.path(), "a"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // B1: queued behind A.
    let b1_layout = layout.clone();
    let b1_repo = b.name.clone();
    let b1_conn = tokio::spawn(async move {
        let mut client = Client::connect_as_operator(&b1_layout).await.unwrap();
        client
            .call("verify.run", json!({"repo": b1_repo, "check": "go1"}))
            .await
    });
    poll_status_until(&mut client, "B1 is queued", |s| {
        host_executing(s) == 1 && host_waiting(s) == 1
    })
    .await;

    // B2: queued behind B1 — proves the queue holds more than one waiter.
    let b2_call = spawn_verify(layout.clone(), b.name.clone(), "go2".into());
    poll_status_until(&mut client, "B1 and B2 are both queued", |s| {
        host_executing(s) == 1 && host_waiting(s) == 2
    })
    .await;
    assert_not_started_yet(&pid_path(shared.path(), "b1"), "B1 must still be queued");
    assert_not_started_yet(&pid_path(shared.path(), "b2"), "B2 must still be queued");

    // Cancel the QUEUED request B1 by killing the connection carrying it
    // (server.rs's dispatch_watching_disconnect cancels the exact
    // registration this call made). B1 never spawned a process at all —
    // this is a genuinely queued cancellation, not an executing one.
    // `JoinHandle::abort` (NOT a bare `drop`, which only detaches a tokio
    // task without stopping it) is what actually tears down the client
    // connection this task owns.
    b1_conn.abort();
    let _ = b1_conn.await;
    let b1_outcome = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let s = status(&mut client).await;
            if host_waiting(&s) == 1 && host_executing(&s) == 1 {
                return s;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("B1's cancellation must drop the aggregate waiting count from 2 to 1");
    assert_eq!(
        host_executing(&b1_outcome),
        1,
        "cancelling a QUEUED request must never touch the checked-out permit A still holds: \
         {b1_outcome}"
    );
    assert!(
        !pid_path(shared.path(), "b1").exists(),
        "B1 was genuinely queued — it must never have spawned a real process at all"
    );

    // Cancel the EXECUTING request A. Its real child process must actually
    // die, freeing the one permit for the next queued peer (B2). Same
    // `abort`-not-`drop` reasoning as B1's cancellation above.
    a_conn.abort();
    let _ = a_conn.await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if Command::new("kill")
            .args(["-0", &a_pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| !s.success())
            .unwrap_or(true)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cancelling A's executing verify.run must reap its real child process (pid {a_pid})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    // No permit leak, and B2 (the still-waiting peer) genuinely progresses.
    wait_for_start(&pid_path(shared.path(), "b2")).await;
    poll_status_until(
        &mut client,
        "B2 now holds the permit A's cancellation freed",
        |s| host_executing(s) == 1 && host_waiting(s) == 0,
    )
    .await;

    // Durable cancellation evidence for the executing cancellation.
    let reopened = Space::open(&layout.db_path()).unwrap();
    let cancellations = reopened
        .scan(
            &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                .identity("verification_cancelled"),
        )
        .unwrap();
    assert!(
        !cancellations.is_empty(),
        "at least A's executing cancellation must be durably recorded"
    );

    release(shared.path(), "b2");
    let b2_result = b2_call.await.unwrap();
    assert_eq!(b2_result["exit"], json!(0), "{b2_result:#?}");

    // Read-only status kept responding throughout the entire sequence above
    // (every `status(&mut client).await` call already proved this); confirm
    // capacity fully and correctly drains at the end, with no leaked permit.
    poll_status_until(&mut client, "capacity fully drains, no leak", |s| {
        host_executing(s) == 0 && host_waiting(s) == 0
    })
    .await;
}
