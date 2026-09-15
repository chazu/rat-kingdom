//! P3.2 (TKT-nasif-danob-sirok): the real native CLI/RPC journey for
//! weighted classes and fair progress layered on top of P3.1's aggregate
//! host-wide verification admission cap (`host_verification_aggregate_cap.rs`
//! already covers the plain-counting P3.1 baseline; this file exercises only
//! what P3.2 adds).
//!
//! Two independently registered fixture repos, bounded controlled named
//! checks (a shell one-liner that writes its own pid, then blocks on an
//! explicit release file the test controls — never a fixed sleep as the
//! success criterion). Every concurrency assertion is driven by polling the
//! daemon's own native `status` RPC (`verification_host`) until a real
//! condition holds, bounded by a deadline.
//!
//! Scenarios:
//! - `weight_two_consumes_two_of_an_aggregate_two_cap`: a check configured
//!   with weight 2 occupies the entire aggregate=2 cap alone, and a
//!   concurrent weight-1 check on a different repo queues behind it —
//!   proving weight is atomic (never partially admitted) and shared across
//!   repos.
//! - `guard_class_progresses_while_general_pool_is_saturated`: a cheap
//!   `guard`-class check keeps making progress via its own reserved lane
//!   while a long unclassified check saturates the general pool — the exact
//!   head-of-line scenario the ticket's queue evidence describes — while an
//!   ORDINARY second unclassified request still queues normally, proving
//!   the reserve is bounded, not an unlimited bypass.
//! - `reserved_lane_saturated_falls_through_to_the_general_pool`: once a
//!   class's own reserve is fully occupied, the next same-class request
//!   falls through to the same bounded general-pool queue every
//!   unclassified request already uses — it never blocks indefinitely on
//!   its own exhausted reserve.

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

/// Same barrier-controlled check body as `host_verification_aggregate_cap.rs`:
/// writes its own pid the instant it starts (proving genuine execution, not
/// merely admission), then blocks until the test deposits a release file.
fn barrier_check_body(shared: &Path, marker: &str) -> String {
    let shared = shared.display();
    format!(
        r#"echo $$ > "{shared}/{marker}.pid"; for i in $(seq 1 600); do [ -f "{shared}/{marker}.release" ] && exit 0; sleep 0.05; done; echo "barrier {marker} never released" 1>&2; exit 9"#
    )
}

fn cue_command(body: &str) -> String {
    body.replace('\\', "\\\\").replace('"', "\\\"")
}

fn write_barrier_checks(repo: &Path, shared: &Path, checks: &[(&str, &str)]) {
    let mut cue = String::from("checks: [\n");
    for (name, marker) in checks {
        let body = barrier_check_body(shared, marker);
        cue.push_str(&format!(
            "    {{name: \"{name}\", command: \"{}\", timeout: \"30s\", environmentPolicy: \"strip_rk_spawn\"}},\n",
            cue_command(&body)
        ));
    }
    cue.push_str("]\n");
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), cue).unwrap();
}

fn release(shared: &Path, marker: &str) {
    std::fs::write(shared.join(format!("{marker}.release")), b"go").unwrap();
}

fn pid_path(shared: &Path, marker: &str) -> std::path::PathBuf {
    shared.join(format!("{marker}.pid"))
}

const POLL_DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(30);

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

fn assert_not_started_yet(path: &Path, context: &str) {
    assert!(
        !path.exists(),
        "{context}: {} must not exist yet — the request should still be queued, not executing",
        path.display()
    );
}

const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(5);

async fn status(client: &mut Client) -> Value {
    tokio::time::timeout(RPC_CALL_TIMEOUT, client.call("status", json!({})))
        .await
        .expect("status RPC must respond within its own bounded timeout, not hang indefinitely")
        .unwrap()
}

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

fn class_executing(s: &Value, class: &str) -> u64 {
    s["verification_host"]["classes"][class]["executing"]
        .as_u64()
        .unwrap_or(0)
}

/// Spins up a daemon with an aggregate limit AND a P3.2 weight/class
/// policy — mirrors `host_verification_aggregate_cap.rs`'s
/// `spawn_daemon_with_limits`, extended with the two calls this ticket adds.
async fn spawn_daemon_with_policy(
    layout: &Layout,
    aggregate_limit: u32,
    check_weight: HashMap<String, u32>,
    check_class: HashMap<String, String>,
    class_reserve: HashMap<String, u32>,
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
    daemon.set_verification_admission_aggregate_limit(aggregate_limit);
    daemon
        .set_verification_admission_class_policy(check_weight, check_class, class_reserve)
        .expect("test-constructed policy must validate");
    tokio::spawn(daemon.run());
}

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

/// A check configured with weight 2 occupies BOTH units of an aggregate=2
/// cap alone; a concurrent weight-1 check on a genuinely different repo
/// must queue behind it — proving weight is atomic (acquired via
/// `acquire_many_owned`, never partially) and shared host-wide, not
/// per-repo.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn weight_two_consumes_two_of_an_aggregate_two_cap() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let heavy_repo = prepare_repo(shared.path(), &[("heavy", "heavy")]);
    let light_repo = prepare_repo(shared.path(), &[("light", "light")]);

    spawn_daemon_with_policy(
        &layout,
        2,
        HashMap::from([("heavy".to_string(), 2)]),
        HashMap::new(),
        HashMap::new(),
    )
    .await;
    let mut client = connect(&layout).await;
    register(&mut client, &heavy_repo.name, heavy_repo.dir.path()).await;
    register(&mut client, &light_repo.name, light_repo.dir.path()).await;

    let heavy_call = spawn_verify(layout.clone(), heavy_repo.name.clone(), "heavy".into());
    wait_for_start(&pid_path(shared.path(), "heavy")).await;
    poll_status_until(
        &mut client,
        "the weight-2 check alone occupies the whole aggregate=2 cap",
        |s| host_executing(s) == 2,
    )
    .await;

    let light_call = spawn_verify(layout.clone(), light_repo.name.clone(), "light".into());
    poll_status_until(
        &mut client,
        "the weight-1 check on a different repo queues behind the saturated aggregate",
        |s| host_executing(s) == 2 && host_waiting(s) == 1,
    )
    .await;
    assert_not_started_yet(
        &pid_path(shared.path(), "light"),
        "weight 2 must occupy the entire aggregate=2 cap, leaving nothing for the light check",
    );

    release(shared.path(), "heavy");
    assert_eq!(heavy_call.await.unwrap()["exit"], json!(0));

    wait_for_start(&pid_path(shared.path(), "light")).await;
    poll_status_until(
        &mut client,
        "the light check now occupies the freed capacity",
        |s| host_executing(s) == 1 && host_waiting(s) == 0,
    )
    .await;
    release(shared.path(), "light");
    assert_eq!(light_call.await.unwrap()["exit"], json!(0));

    poll_status_until(&mut client, "capacity fully drains", |s| {
        host_executing(s) == 0 && host_waiting(s) == 0
    })
    .await;
}

/// A cheap `guard`-class check keeps making progress via its own reserved
/// lane while a long unclassified check saturates the general pool — the
/// head-of-line scenario the ticket's queue evidence describes. A SECOND,
/// ordinary unclassified request fired at the same time still queues
/// normally: the reserve benefits only its own class, never becomes a
/// blanket bypass.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn guard_class_progresses_while_general_pool_is_saturated() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let slow_repo = prepare_repo(shared.path(), &[("slow", "slow1"), ("slow2", "slow2")]);
    let guard_repo = prepare_repo(shared.path(), &[("quick", "quick")]);

    // aggregate=2, one unit reserved for the "guard" class -> general pool
    // is left with exactly 1 unit, same size as the long check alone needs.
    spawn_daemon_with_policy(
        &layout,
        2,
        HashMap::new(),
        HashMap::from([("quick".to_string(), "guard".to_string())]),
        HashMap::from([("guard".to_string(), 1)]),
    )
    .await;
    let mut client = connect(&layout).await;
    register(&mut client, &slow_repo.name, slow_repo.dir.path()).await;
    register(&mut client, &guard_repo.name, guard_repo.dir.path()).await;

    // The long check saturates the entire general pool (limit 1).
    let slow_call = spawn_verify(layout.clone(), slow_repo.name.clone(), "slow".into());
    wait_for_start(&pid_path(shared.path(), "slow1")).await;
    poll_status_until(
        &mut client,
        "the long check saturates the general pool",
        |s| host_executing(s) == 1,
    )
    .await;

    // A second, ordinary unclassified request genuinely queues behind it —
    // the general pool is exhausted, exactly as under plain P3.1.
    let slow2_call = spawn_verify(layout.clone(), slow_repo.name.clone(), "slow2".into());
    poll_status_until(
        &mut client,
        "an ordinary second request queues behind the saturated general pool",
        |s| host_executing(s) == 1 && host_waiting(s) == 1,
    )
    .await;
    assert_not_started_yet(
        &pid_path(shared.path(), "slow2"),
        "an unclassified request must still queue normally behind the general pool",
    );

    // The guard-class check nonetheless makes genuine progress via its own
    // reserved lane, without ever entering the general pool's wait queue.
    let quick_call = spawn_verify(layout.clone(), guard_repo.name.clone(), "quick".into());
    wait_for_start(&pid_path(shared.path(), "quick")).await;
    let both = poll_status_until(
        &mut client,
        "the guard-class check runs concurrently with the saturated general pool",
        |s| host_executing(s) == 2 && class_executing(s, "guard") == 1,
    )
    .await;
    assert_eq!(
        host_waiting(&both),
        1,
        "the guard check's own reserved-lane admission must never touch the general \
         pool's waiting count: {both}"
    );

    release(shared.path(), "quick");
    assert_eq!(quick_call.await.unwrap()["exit"], json!(0));
    poll_status_until(&mut client, "the guard lane drains back to 0", |s| {
        class_executing(s, "guard") == 0
    })
    .await;

    release(shared.path(), "slow1");
    assert_eq!(slow_call.await.unwrap()["exit"], json!(0));

    wait_for_start(&pid_path(shared.path(), "slow2")).await;
    release(shared.path(), "slow2");
    assert_eq!(slow2_call.await.unwrap()["exit"], json!(0));

    poll_status_until(&mut client, "capacity fully drains", |s| {
        host_executing(s) == 0 && host_waiting(s) == 0
    })
    .await;
}

/// Once a class's own reserve is fully occupied, the NEXT same-class
/// request falls through to the ordinary general-pool queue — a saturated
/// fast lane degrades to shared admission, it never blocks indefinitely on
/// its own exhausted reserve, and it never steals more than its configured
/// share.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reserved_lane_saturated_falls_through_to_the_general_pool() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let repo = prepare_repo(shared.path(), &[("quick1", "q1"), ("quick2", "q2")]);

    // aggregate=2, guard reserve=1 -> general pool also sized 1.
    spawn_daemon_with_policy(
        &layout,
        2,
        HashMap::new(),
        HashMap::from([
            ("quick1".to_string(), "guard".to_string()),
            ("quick2".to_string(), "guard".to_string()),
        ]),
        HashMap::from([("guard".to_string(), 1)]),
    )
    .await;
    let mut client = connect(&layout).await;
    register(&mut client, &repo.name, repo.dir.path()).await;

    let call1 = spawn_verify(layout.clone(), repo.name.clone(), "quick1".into());
    wait_for_start(&pid_path(shared.path(), "q1")).await;
    poll_status_until(
        &mut client,
        "quick1 occupies the guard class's one reserved unit",
        |s| class_executing(s, "guard") == 1,
    )
    .await;

    // quick2 is also guard-class, but the reserve is full: it must fall
    // through to the general pool (which has its own free unit) and START
    // running there rather than blocking on the exhausted reserve.
    let call2 = spawn_verify(layout.clone(), repo.name.clone(), "quick2".into());
    wait_for_start(&pid_path(shared.path(), "q2")).await;
    poll_status_until(
        &mut client,
        "quick2 falls through to the general pool and runs concurrently",
        |s| host_executing(s) == 2 && class_executing(s, "guard") == 1,
    )
    .await;

    release(shared.path(), "q1");
    assert_eq!(call1.await.unwrap()["exit"], json!(0));
    release(shared.path(), "q2");
    assert_eq!(call2.await.unwrap()["exit"], json!(0));

    poll_status_until(&mut client, "capacity fully drains", |s| {
        host_executing(s) == 0 && host_waiting(s) == 0 && class_executing(s, "guard") == 0
    })
    .await;
}
