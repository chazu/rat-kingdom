//! WIP-4 verification-admission saturation regression
//! (TKT-01M0HNFDHR7GHRDE618RB77VX6), closing the load-flake parent
//! TKT-01M0D2APS09AXKB4AHAYHCPSPX.
//!
//! Deterministic, fixture-backed (fake harness / real check subprocesses —
//! no paid model agents): drives more concurrent `verify.run` requests than
//! a WIP=4 `verification_admission_limit_by_repo` cap against one repo, and
//! proves (a) peak concurrent check execution never exceeds the cap, (b)
//! every queued check eventually starts (none starved), and (c) one
//! deliberately failing check's exact exit status/verdict is reported for
//! IT alone, never coalesced with its siblings' passing results.
//!
//! Closest existing template:
//! `capacity_lanes_dispatch_load.rs` (per-repo admission lanes under load)
//! and `workflow_exec.rs`'s
//! `landing_gate_and_verify_run_share_one_admission_bound_for_the_same_repo_name`
//! (marker-file peak-concurrency proof).

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

// `RK_FAKE_HARNESS_CMD` is a process-global env var and `#[tokio::test]`
// bodies in one binary run concurrently by default — without this lock, a
// sibling test's `set_var` can clobber this one's between its own `set_var`
// and the moment its spawned agent's harness process actually reads it
// (same race `managed_verification_cancel_e2e.rs`'s own `HARNESS_ENV_LOCK`
// guards against). Held for the whole test body by every test below that
// touches the var.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

/// Escapes a shell command string for embedding inside a CUE double-quoted
/// `command: "..."` field.
fn cue_command(body: &str) -> String {
    body.replace('\\', "\\\\").replace('"', "\\\"")
}

/// One marker/peak-concurrency check body: on start, drops its own pid
/// marker into `shared`, snapshots how many markers currently exist (the
/// live concurrency count at that instant) into `peak.log`, records that it
/// started at all into `started.log`, sleeps briefly so overlapping
/// invocations have a real window to collide in, then removes its marker.
/// `fail` appends a distinct nonzero exit with a distinct stderr line, so
/// the test can prove that ONE check's red result is reported exactly,
/// never swallowed into its siblings' green ones.
fn marker_check_body(shared: &Path, fail: bool) -> String {
    let shared = shared.display();
    let tail = if fail {
        r#"; echo "sat-distinct-failure" 1>&2; exit 7"#
    } else {
        ""
    };
    format!(
        r#"f="{shared}/m-$$"; touch "$f"; n=$(ls "{shared}"/m-* 2>/dev/null | wc -l | tr -d ' '); echo "$n" >> "{shared}/peak.log"; echo started >> "{shared}/started.log"; sleep 0.3; rm -f "$f"{tail}"#
    )
}

/// `n` checks named `sat-0`..`sat-{n-1}` sharing one repo's verification
/// admission lane; the LAST one is the deliberately failing check.
fn write_saturation_checks(repo: &Path, shared: &Path, n: usize, shared_cargo_target: bool) {
    let mut checks = String::from("checks: [\n");
    for i in 0..n {
        let fail = i == n - 1;
        let body = marker_check_body(shared, fail);
        // General admission applies equally to shell and Cargo checks.
        checks.push_str(&format!(
            "    {{name: \"sat-{i}\", command: \"{}\", timeout: \"10s\", environmentPolicy: \"strip_rk_spawn\", sharedCargoTarget: {shared_cargo_target}}},\n",
            cue_command(&body)
        ));
    }
    checks.push_str("]\n");
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), checks).unwrap();
}

async fn run_verify(layout: &Layout, repo: &str, check: &str) -> Value {
    let mut client = Client::connect_as_operator(layout).await.unwrap();
    client
        .call("verify.run", json!({"repo": repo, "check": check}))
        .await
        .unwrap_or_else(|e| panic!("verify.run({repo}, {check}) failed: {e}"))
}

const SATURATION_DEADLINE: Duration = Duration::from_secs(20);
const N_CHECKS: usize = 8;
const WIP_LIMIT: u32 = 4;

/// The core WIP-4 saturation proof: `N_CHECKS` (8) concurrent `verify.run`
/// requests against one repo whose `verification_admission_limit_by_repo`
/// is capped at 4 (an eight-core host's declared policy). Proves admission
/// stays within policy (peak concurrent execution <= 4), every queued check
/// eventually starts (all 8 record a `started` line, none starved out),
/// and the one deliberately failing check's exact red result (`exit: 7`)
/// is attributed to it alone — every other check still reports `exit: 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wip4_admission_saturation_stays_bounded_starves_nothing_and_keeps_exact_child_failures_red(
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());
    let shared = tempfile::tempdir().unwrap();
    write_saturation_checks(repo_dir.path(), shared.path(), N_CHECKS, false);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = Space::open(&layout.db_path()).unwrap();
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_verification_admission_limits(0, HashMap::from([(repo_name.clone(), WIP_LIMIT)]));
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // Fire all N_CHECKS concurrently, each over its own connection — a
    // single `Client` serializes its own calls, so genuine overlap needs
    // one connection per in-flight request (same technique
    // `capacity_lanes_dispatch_load.rs`'s concurrent `verify.run` task
    // uses).
    let mut handles = Vec::new();
    for i in 0..N_CHECKS {
        let layout = layout.clone();
        let repo_name = repo_name.clone();
        handles.push(tokio::spawn(async move {
            let result = tokio::time::timeout(
                SATURATION_DEADLINE,
                run_verify(&layout, &repo_name, &format!("sat-{i}")),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "sat-{i} never completed within {SATURATION_DEADLINE:?} — admission starved it"
                )
            });
            (i, result)
        }));
    }

    let mut results: Vec<(usize, Value)> = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }
    results.sort_by_key(|(i, _)| *i);

    for (i, result) in &results {
        if *i == N_CHECKS - 1 {
            assert_eq!(
                result["exit"],
                json!(7),
                "the deliberately failing check sat-{i} must report its own exact exit \
                 status, not a coalesced/misattributed one: {result:#?}"
            );
            assert_eq!(result["verdict"], json!("fail"), "{result:#?}");
        } else {
            assert_eq!(
                result["exit"],
                json!(0),
                "check sat-{i} must pass unaffected by its failing sibling: {result:#?}"
            );
            assert_eq!(result["verdict"], json!("pass"), "{result:#?}");
        }
    }

    let started = std::fs::read_to_string(shared.path().join("started.log")).unwrap_or_default();
    assert_eq!(
        started.lines().count(),
        N_CHECKS,
        "every one of the {N_CHECKS} queued checks must eventually start — a starved check \
         would leave started.log short: {started:?}"
    );

    let peak_log = std::fs::read_to_string(shared.path().join("peak.log")).unwrap_or_default();
    let peak: usize = peak_log
        .lines()
        .filter_map(|l| l.trim().parse::<usize>().ok())
        .max()
        .unwrap_or(0);
    assert!(
        peak <= WIP_LIMIT as usize,
        "admission must stay within the repository's WIP={WIP_LIMIT} policy at all times: \
         observed peak concurrent execution was {peak}, log: {peak_log:?}"
    );
    assert!(
        peak >= 2,
        "the test must actually exercise real overlap to be a meaningful saturation proof, \
         not just 8 checks running one at a time: observed peak was only {peak}"
    );
    // Inspect through a new database handle, independently of the RPC result:
    // red checks and their exact diagnostics must be durable under saturation.
    let reopened = Space::open(&layout.db_path()).unwrap();
    let failures = reopened
        .scan(
            &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Artifact)
                .scope(&repo_name)
                .identity("gate-failure"),
        )
        .unwrap();
    assert_eq!(failures.len(), 1, "one red child must persist one failure");
    let failure = &failures[0].payload;
    assert_eq!(failure["exit"], 7);
    assert_eq!(failure["verdict"], "fail");
    assert_eq!(failure["timed_out"], false);
    assert!(failure["stderr_tail"]
        .as_str()
        .unwrap()
        .contains("sat-distinct-failure"));
    let timings = reopened
        .scan(
            &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                .scope(&repo_name)
                .identity("verification_admission"),
        )
        .unwrap();
    assert_eq!(timings.len(), N_CHECKS, "timing includes the failed check");
    assert!(timings
        .iter()
        .any(|t| t.payload["queue_wait_ms"].as_u64().unwrap() > 0));
}

/// `rk-daemon` doesn't build the `rk` binary itself (no build-time
/// dependency on `rk-cli`), so `cargo test -p rk-daemon` alone never
/// populates it — same rationale/fallback as
/// `managed_verification_cancel_e2e.rs::rk_bin`.
fn rk_bin() -> String {
    let path = std::env::var("CARGO_BIN_EXE_rk").unwrap_or_else(|_| {
        let target_dir = std::env::var("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| support::workspace_root().join("target"));
        target_dir
            .join("debug")
            .join("rk")
            .to_string_lossy()
            .into_owned()
    });
    assert!(
        Path::new(&path).exists(),
        "rk binary not found at {path} — build it first (`cargo build -p rk-cli --bin rk`) or \
         run `cargo test --workspace`, which builds every workspace member including rk-cli."
    );
    path
}

fn install_long_verify_check(dir: &Path) {
    let rk_dir = dir.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("checks.cue"),
        r#"checks: [
    {name: "verify", command: "echo $$ > verify.pid; sleep 20", timeout: "30s", environmentPolicy: "strip_rk_spawn"},
]
"#,
    )
    .unwrap();
    git(dir, &["add", ".rk/checks.cue"]);
    git(dir, &["commit", "-m", "test: install long verify check"]);
}

/// One fake harness script, behaviour selected by `$RK_FAKE_PROMPT` (the
/// task text) — same technique `supervisor_sweep.rs`'s `COMBINED_FAKE`
/// uses. Neither branch ever declares `rk_done`; both are meant to be acted
/// on by the supervisor's liveness sweep, not to complete normally.
///
/// - `*alive-verifier*`: backgrounds a REAL `rk verify` call (through the
///   real `rk` binary) against a check that writes its own pid then sleeps
///   20s, waits for that pid file to exist, then goes silent itself — no
///   more harness output, but a genuinely live verifier descendant process
///   tree hangs off it.
/// - anything else: goes silent immediately with no descendants at all —
///   the plain STUCK case `supervisor_sweep.rs` already covers, included
///   here as the negative control proving the sweep still reclaims a truly
///   dead/silent generation while its live-verifier sibling survives.
fn liveness_fake(rk: &str) -> String {
    format!(
        r#"
echo '{{"type":"system","subtype":"init","session_id":"liveness-fake"}}'
read -r _prompt
case "$RK_FAKE_PROMPT" in
  *alive-verifier*)
    '{rk}' verify --repo "$RK_REPO" > verify-rpc-output.txt 2>&1 &
    for i in $(seq 1 200); do
      [ -f verify.pid ] && break
      sleep 0.05
    done
    sleep 30
    ;;
  *)
    sleep 120
    ;;
esac
"#
    )
}

fn process_alive(pid: i32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// A start-time+command snapshot for one real OS process, captured once
/// while it is known-good. A pid alone is not an identity — the OS can
/// recycle it for an unrelated process — so re-checking this snapshot
/// before signaling is what tells "still the same process" apart from
/// "coincidentally the same number". Mirrors, in test-only `ps` terms,
/// the pid+start-time signature discipline
/// `workflow_exec::reap_stale_managed_children` (the actual production
/// recovery mechanism this test exercises) uses internally — that
/// function is `pub(crate)` and unreachable from this external test
/// crate, so this is a from-scratch equivalent, not a shared
/// implementation. `lstart` is immutable for a process's whole lifetime,
/// so an unchanged signature is as strong a same-process proof as this
/// test can get without reading `/proc` (unavailable on this host's OS).
fn process_signature(pid: i32) -> Option<String> {
    let output = Command::new("ps")
        .args(["-o", "lstart=,command=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

/// Best-effort test-hygiene safety net for the exact real check subprocess
/// a restart test observes, armed for the window between confirming a
/// daemon's physical death and confirming the replacement daemon's own
/// recovery reclaimed the orphan it left behind — an early panic in that
/// window (e.g. a failed assertion) must not leak a live background
/// process. This is NOT part of, and never races, the actual recovery
/// mechanism under test (`workflow_exec::reap_stale_managed_children`'s
/// own pid+signature check) — on the successful path that mechanism
/// already kills the process well before this guard ever drops, so
/// `process_alive` is false and `drop` is a no-op.
///
/// Fails closed: `signature` is captured once, at construction, from a
/// pid this test just confirmed alive; if that capture ever comes back
/// empty (a narrow liveness/ps race), `signature` is `None` and `drop`
/// never signals, full stop — there is nothing trustworthy left to compare
/// against. When a signature was captured, `drop` re-derives it fresh and
/// only signals if it is BYTE-IDENTICAL to the one captured at
/// construction (rules out pid reuse) AND still names this exact test
/// invocation's unique fixture path (rules out matching a sibling
/// invocation's own, differently-pathed "verify.pid" check — every
/// invocation of this test shares that literal filename, so it alone is
/// not a unique identity).
struct OwnedCheckCleanup {
    pid: i32,
    signature: Option<String>,
    fixture_path: String,
}

impl OwnedCheckCleanup {
    fn new(pid: i32, fixture_path: impl Into<String>) -> Self {
        Self {
            pid,
            signature: process_signature(pid),
            fixture_path: fixture_path.into(),
        }
    }
}

impl Drop for OwnedCheckCleanup {
    fn drop(&mut self) {
        let Some(expected) = &self.signature else {
            return;
        };
        if !process_alive(self.pid) {
            return;
        }
        let Some(current) = process_signature(self.pid) else {
            return;
        };
        if &current != expected || !current.contains(&self.fixture_path) {
            return;
        }
        // Negative pid: the production check spawns with `.process_group(0)`
        // (`managed_verification.rs::spawn_check_child`), so this owned
        // group's `sleep` descendant — which does not itself carry the
        // `verify.pid`-writing `sh` leader's own signature — is reached too,
        // bounded to this exact confirmed-identity group and never a bare
        // or reused-pid signal.
        let _ = Command::new("kill")
            .args(["-9", &format!("-{}", self.pid)])
            .status();
    }
}

async fn wait_for_pid(path: &Path) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(pid) = text.trim().parse() {
                return pid;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the check's real child never wrote its own pid to {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// "live verifiers are not reaped" / "dead or transport-unhealthy
/// generations are reclaimed" (TKT-01M0HNF2HR9Y0PY44RHY4Q245P's liveness
/// evidence, exercised here as part of the saturation regression): one
/// agent goes silent but has a real, live `rk verify` descendant process
/// tree — it must survive the supervisor's sweep untouched. A second agent
/// goes silent with NO descendants at all — it must be reclaimed as stuck
/// within the configured grace window. Same tight-threshold technique as
/// `supervisor_sweep.rs`.
#[tokio::test]
async fn live_verifier_descendant_survives_the_sweep_while_a_silent_dead_generation_is_reclaimed() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());
    install_long_verify_check(repo_dir.path());

    let rk = rk_bin();
    std::env::set_var("RK_FAKE_HARNESS_CMD", liveness_fake(&rk));

    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_sweep_config(rk_core::config::SupervisorConfig {
        enabled: true,
        interval_secs: 1,
        stuck_after_secs: 1,
        burn_usd_per_min: 0.0,
        kill_grace_secs: 2,
        ..rk_core::config::SupervisorConfig::default()
    });
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let alive = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "alive-verifier-1",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let alive_name = alive["agent"]["name"].as_str().unwrap().to_string();
    let alive_worktree = std::path::PathBuf::from(alive["agent"]["worktree"].as_str().unwrap());

    let dead = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "silent-dead-1",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let dead_name = dead["agent"]["name"].as_str().unwrap().to_string();

    let alive_pid = wait_for_pid(&alive_worktree.join("verify.pid")).await;
    assert!(
        process_alive(alive_pid),
        "the alive agent's own real verify child must be running before the sweep can act"
    );

    let mut dead_failed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let status = client
            .call("agent.status", json!({"name": &dead_name}))
            .await
            .unwrap();
        if status["agent"]["state"].as_str() == Some("failed") {
            dead_failed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        dead_failed,
        "the silent, descendant-less generation must eventually be reclaimed as stuck"
    );

    let alive_status = client
        .call("agent.status", json!({"name": &alive_name}))
        .await
        .unwrap();
    assert!(
        matches!(
            alive_status["agent"]["state"].as_str(),
            Some("spawning") | Some("running")
        ),
        "an agent with a live verifier descendant must NOT be reaped even while its own \
         output is silent: {alive_status}"
    );
    assert!(
        process_alive(alive_pid),
        "the live verifier's real child process must still be alive — the sweep must never \
         have touched it"
    );

    let obstacles = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    let stuck_kinds: Vec<String> = obstacles["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["payload"]["type"].as_str().unwrap_or("").to_string())
        .collect();
    assert!(
        stuck_kinds.iter().any(|k| k == "stuck"),
        "a stuck obstacle must have fired for the reclaimed dead generation: {stuck_kinds:?}"
    );

    // Best-effort cleanup: the live verifier's real child would otherwise
    // hold its sleep for the rest of the suite's run.
    let _ = Command::new("kill")
        .args(["-9", &alive_pid.to_string()])
        .status();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A check body that proves genuine overlap with a *sibling* check running
/// in another repo, without relying on wall-clock margins: it drops its own
/// marker into a directory shared across both repos, waits just long enough
/// for the sibling to have started, then records whether the sibling's
/// marker is *still present* into `overlap.log` before removing its own.
/// If the two checks ran serially (one queued behind the other), the first
/// one's marker would already be gone by the time the second checks for it
/// — same marker-file technique as `marker_check_body` above, adapted for
/// two distinct repos sharing one directory instead of `N` checks sharing
/// one repo's admission lane.
fn cross_repo_marker_body(shared: &Path, own: &str, other: &str) -> String {
    let shared = shared.display();
    format!(
        r#"touch "{shared}/{own}.marker"; sleep 0.1; if [ -f "{shared}/{other}.marker" ]; then echo "saw-{other}" >> "{shared}/overlap.log"; else echo "no-{other}" >> "{shared}/overlap.log"; fi; sleep 0.2; rm -f "{shared}/{own}.marker""#
    )
}

fn install_marker_check(dir: &Path, shared: &Path, own: &str, other: &str) {
    let rk_dir = dir.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("checks.cue"),
        format!(
            "checks: [\n    {{name: \"verify\", command: \"{}\", timeout: \"10s\", environmentPolicy: \"strip_rk_spawn\", sharedCargoTarget: true}},\n]\n",
            cue_command(&cross_repo_marker_body(shared, own, other))
        ),
    )
    .unwrap();
}

/// "work in separate repositories can proceed concurrently": two distinct
/// repos, each with its own tight `verification_admission_limit_by_repo` of
/// 1, must NOT contend against each other for that permit — the admission
/// lane is keyed per repo, not a single cross-repo lock. Proven by marker
/// files rather than wall clock (matching this file's other saturation
/// tests, per TKT-01M0D2APS09AXKB4AHAYHCPSPX's wall-clock-flake history):
/// each repo's check drops a marker, waits, then checks whether its
/// sibling's marker is *still present* — if the two secretly shared one
/// lock, the second check would only start after the first had already
/// removed its marker, and neither side would ever observe the other's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_repo_verification_admission_is_independent_and_proceeds_concurrently() {
    let home = tempfile::tempdir().unwrap();
    let repo_a_dir = tempfile::tempdir().unwrap();
    let repo_b_dir = tempfile::tempdir().unwrap();
    let repo_a_name = init_repo(repo_a_dir.path());
    let repo_b_name = init_repo(repo_b_dir.path());
    let shared = tempfile::tempdir().unwrap();
    install_marker_check(repo_a_dir.path(), shared.path(), "a", "b");
    install_marker_check(repo_b_dir.path(), shared.path(), "b", "a");

    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_verification_admission_limits(
        0,
        HashMap::from([(repo_a_name.clone(), 1), (repo_b_name.clone(), 1)]),
    );
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_a_name, "path": repo_a_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    client
        .call(
            "repo.add",
            json!({"name": &repo_b_name, "path": repo_b_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let (ra, rb) = tokio::join!(
        run_verify(&layout, &repo_a_name, "verify"),
        run_verify(&layout, &repo_b_name, "verify"),
    );

    assert_eq!(ra["exit"], json!(0), "{ra:#?}");
    assert_eq!(rb["exit"], json!(0), "{rb:#?}");

    let overlap_log =
        std::fs::read_to_string(shared.path().join("overlap.log")).unwrap_or_default();
    let lines: Vec<&str> = overlap_log.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "both checks must have run and recorded an overlap observation: {overlap_log:?}"
    );
    assert!(
        lines.contains(&"saw-b") && lines.contains(&"saw-a"),
        "two independent repos' verification admission lanes must run concurrently, not \
         contend for one shared lock: each check must have observed its sibling's marker \
         still present while it was running — if the lane were shared, the second check \
         would only start after the first had already removed its marker: {overlap_log:?}"
    );
}

/// "no shared-target ENOENT occurs": a check opted into `sharedCargoTarget`
/// is serialized by a SEPARATE lock (`Supervisor::acquire_test_exec_lock`)
/// from the per-repo admission queue — proven here by giving the repo
/// admission headroom well above the number of concurrent checks (4 vs 3),
/// so admission alone would let all 3 run at once, and showing peak
/// concurrent execution is still exactly 1. That serialization is what
/// prevents the concurrent-CARGO_TARGET_DIR race that used to produce
/// ENOENT under real load — proven here as "the race window never opens"
/// (peak==1, zero failing runs) rather than by reproducing the ENOENT
/// itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_cargo_target_checks_serialize_to_one_regardless_of_admission_headroom() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());
    let shared = tempfile::tempdir().unwrap();
    const N: usize = 3;
    write_saturation_checks(repo_dir.path(), shared.path(), N, true);
    // write_saturation_checks's last check deliberately fails (exit 7) —
    // irrelevant here since this test only cares about peak concurrency,
    // but drop its stderr expectation by not asserting on exit codes below.

    let layout = Layout::at(home.path());
    let space = Space::open_in_memory().unwrap();
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    // WIP-4 admission headroom deliberately GREATER than the 3 concurrent
    // checks below — admission alone would not serialize them.
    daemon.set_verification_admission_limits(0, HashMap::from([(repo_name.clone(), 4)]));
    daemon.set_shared_cargo_target(true);
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..N {
        let layout = layout.clone();
        let repo_name = repo_name.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::timeout(
                SATURATION_DEADLINE,
                run_verify(&layout, &repo_name, &format!("sat-{i}")),
            )
            .await
            .unwrap_or_else(|_| {
                panic!("sat-{i} never completed — the shared-target lock queue starved it")
            })
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let peak_log = std::fs::read_to_string(shared.path().join("peak.log")).unwrap_or_default();
    let peak: usize = peak_log
        .lines()
        .filter_map(|l| l.trim().parse::<usize>().ok())
        .max()
        .unwrap_or(0);
    assert_eq!(
        peak, 1,
        "sharedCargoTarget checks must serialize to exactly one concurrent execution \
         regardless of admission headroom (limit 4, only {N} checks) — the race window that \
         used to produce a shared-target ENOENT must never open: peak was {peak}, log: \
         {peak_log:?}"
    );
}

// --- Mid-queue restart: order, ticket ownership, budget, and lease replay ---
//
// `landing_dedup_atomic.rs`'s `restart_preserves_the_landing_dedup_invariant`
// and `restart_mid_gate_kill_does_not_race_a_second_landing_processed_marker`
// already prove landing-identity dedup survives a restart — but only for ONE
// queued candidate. `managed_verification_cancel_e2e.rs`'s
// `daemon_restart_never_blocks_progress_on_a_run_that_was_in_flight_when_it_died`
// already proves a verification-admission lease never leaks across a
// restart. Neither proves that the durable landing QUEUE's FIFO ORDER across
// TWO candidates survives a restart — that is the one genuinely missing
// assertion this test adds, reusing the same real-daemon-over-socket
// restart idiom (`live_landing_restart.rs`) rather than a new harness, and
// folding in ticket-ownership and budget/cost_usd non-duplication as cheap
// additional assertions on the same two real agents once they exist.
mod fixture;

const RESTART_LANDING_TRIGGER: &str = r#"
triggers: [
    {
        name:   "legacy-landing-on-completion"
        action: "land"
        match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
        maxFires: 20
    },
]
"#;

/// Two policy gates pass instantly; `verify`'s FIRST attempt holds itself
/// open on an explicit release-marker loop rather than a plain `sleep N` —
/// a fixed sleep's "success" path is just elapsed time, and once daemon A
/// (the only thing with any timeout notion for this check) is dead, NOTHING
/// still enforces that timeout: an orphaned `sleep` simply finishes on its
/// own and exits 0, which would silently misreport a broken kill/recovery
/// path as a passing check. The loop instead waits for a `release` marker
/// this test deliberately NEVER creates, bounded by a finite iteration
/// budget that self-terminates with `exit 124` (the conventional
/// `timeout(1)` code) if ever exhausted — so the only two possible
/// outcomes are: killed for real by production recovery (no exit code at
/// all — SIGKILL, not a return), or an explicit, loud, nonzero failure.
/// There is no third path where elapsed time alone reports success. Once
/// `held` exists, EVERY later attempt — including daemon B's own recovery
/// re-run of this same interrupted gate — completes immediately, so
/// restart-recovery marches through the once-real hold with no lingering
/// timing dependency.
///
/// The first attempt also drops its own real shell pid (`$$`) into `shared`
/// before holding — `.process_group(0)`
/// (`managed_verification.rs::spawn_check_child`) makes this pid its own
/// process group leader, the exact pid `ProcessGroupGuard` targets on
/// cancellation. The restart test below reads it back to build a genuine
/// gate-start/release barrier around the real OS process, rather than
/// trusting a same-process task abort (or a naturally-timed sleep) alone to
/// mean the check's child is actually dead by the time daemon B starts.
fn restart_checks(shared: &Path) -> String {
    format!(
        r#"
checks: [
    {{name: "landing-protected-paths", command: "true", timeout: "30s"}},
    {{name: "landing-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{}", timeout: "30s", sharedCargoTarget: true}},
]
"#,
        cue_command(&format!(
            r#"if [ -f "{shared}/held" ]; then exit 0; fi; echo $$ > "{shared}/verify.pid"; touch "{shared}/held"; i=0; while [ ! -f "{shared}/release" ]; do i=$((i+1)); [ "$i" -ge 200 ] && exit 124; sleep 0.1; done"#,
            shared = shared.display()
        ))
    )
}

fn init_repo_restart(dir: &Path, shared: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
    let rk_dir = dir.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), restart_checks(shared)).unwrap();
    git(dir, &["add", ".rk/checks.cue"]);
    git(dir, &["commit", "-m", "add checks registry"]);
    dir.file_name().unwrap().to_string_lossy().to_string()
}

/// A doc-only change under a distinct filename/cost per candidate — routes
/// straight to `Supervisor::land` on a gate pass (`classify_diff`), no
/// reviewer needed, keeping this test's only variable the restart+order, not
/// review tiering. Excludes the `read -r _prompt` line: the combined script
/// below reads the prompt exactly once, before branching.
fn candidate_body(note: &str, cost: f64) -> String {
    format!(
        r#"
mkdir -p docs
echo "note" > docs/{note}.md
git add docs/{note}.md >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "docs: add {note}"
echo '{{"type":"system","subtype":"init","session_id":"sat-restart-{note}"}}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"sat-restart-{note}","total_cost_usd":{cost},"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#
    )
}

/// One static fake-harness script, baked once into daemon A's own OS
/// process environment when it is spawned. A genuinely separate daemon
/// process cannot see this test process's later `std::env::set_var` calls —
/// environment is captured at fork/exec, not shared across processes — so
/// unlike the same-process daemons this file's other tests use, the two
/// candidates below must be told apart by something that genuinely crosses
/// the RPC boundary instead: the `agent.spawn` request's own `prompt`
/// field, echoed back to the script as `$RK_FAKE_PROMPT` — the same
/// technique this file's own `liveness_fake` already uses to vary behavior
/// per spawn against one static script.
fn restart_candidates_fake() -> String {
    format!(
        r#"
read -r _prompt
case "$RK_FAKE_PROMPT" in
  *note-1*)
{body1}
    ;;
  *note-2*)
{body2}
    ;;
esac
"#,
        body1 = candidate_body("note-1", 0.01),
        body2 = candidate_body("note-2", 0.02),
    )
}

/// A real `rk daemon run` process reads its config from disk
/// (`Config::load`), unlike the other tests in this file which hand a
/// `Config` value straight to `Daemon::new` in-process — there is no
/// in-process value to hand a genuinely separate OS process.
fn write_restart_daemon_config(layout: &Layout, repo_name: &str) {
    std::fs::write(
        layout.config_file(),
        format!(
            "[harness]\ndefault = \"fake\"\n\n\
             [policy.verification_admission_limit_by_repo]\n\
             {repo_name:?} = {WIP_LIMIT}\n"
        ),
    )
    .unwrap();
}

/// Launch `rk daemon run` as a genuinely independent OS process against
/// `layout` — not `tokio::spawn(daemon.run())` in this test's own process.
/// `harness_cmd` becomes this process's OWN `RK_FAKE_HARNESS_CMD` — the
/// only way to give a separate daemon process a fake-harness script, since
/// it cannot see this test process's own environment.
/// `kill_on_drop` is this test's own cleanup safety net (an early panic
/// must not leak a live daemon process pointed at a tempdir this function
/// is about to delete); it has nothing to do with the production
/// `ProcessGroupGuard`/`kill_on_drop` tradeoff inside the daemon itself.
fn spawn_daemon_process(layout: &Layout, harness_cmd: &str) -> tokio::process::Child {
    tokio::process::Command::new(rk_bin())
        .args(["daemon", "run"])
        .env("RK_HOME", layout.home())
        .env("RK_FAKE_HARNESS_CMD", harness_cmd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("rk daemon run must spawn as a real OS process")
}

async fn wait_agent_completed(client: &mut Client, name: &str) {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        match status["agent"]["state"].as_str() {
            Some("completed") => return,
            Some("failed") => panic!("rat failed instead of completing: {status}"),
            _ => {}
        }
    }
    panic!("rat {name} never completed");
}

async fn queue_entries(client: &mut Client, repo_name: &str) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "landing_queue_entry"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().cloned().unwrap_or_default()
}

async fn processed_markers(client: &mut Client, repo_name: &str) -> Vec<Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "landing_processed"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().cloned().unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_mid_queue_replays_fifo_order_ticket_ownership_and_budget_without_duplication() {
    // No `HARNESS_ENV_LOCK` needed here: daemon A/B are genuinely separate OS
    // processes below, each given its own `RK_FAKE_HARNESS_CMD` via `.env(..)`
    // at spawn time — never this test process's own (process-global, racy)
    // environment, which is what that lock guards for the other tests in
    // this file.
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let shared = tempfile::tempdir().unwrap();
    let repo_name = init_repo_restart(repo_dir.path(), shared.path());
    let main_before = git(repo_dir.path(), &["rev-parse", "main"]);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::write(
        layout.triggers_dir().join("landing.cue"),
        RESTART_LANDING_TRIGGER,
    )
    .unwrap();
    write_restart_daemon_config(&layout, &repo_name);

    // Daemon A: a genuinely independent OS process (`rk daemon run`), not
    // `tokio::spawn(daemon.run())` in this test's own process — so the kill
    // below is a real crash (no Drop/cascade of any kind runs), and daemon
    // B's startup below exercises the SAME on-disk recovery path a real
    // restart does, rather than a same-process approximation of one.
    let mut child_a =
        spawn_daemon_process(&layout, &fixture::with_rk_done(&restart_candidates_fake()));
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // Candidate 1: ticket-owned spawn, real completion, real reactor
    // `action: "land"` dispatch straight onto the daemon-native
    // `LandingQueue` — no workflow ever runs.
    let ticket1 = client
        .call(
            "ticket.new",
            json!({"title": "restart-order candidate 1", "scope": &repo_name}),
        )
        .await
        .unwrap();
    let ticket1_id = ticket1["ticket"]["identity"].as_str().unwrap().to_string();

    let agent1 = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": &ticket1_id,
                "harness": "fake",
                // Tells daemon A's one static combined script (baked in at
                // spawn time) which candidate body to run — see
                // `restart_candidates_fake`'s doc comment.
                "prompt": "restart candidate note-1",
            }),
        )
        .await
        .unwrap();
    let agent1_name = agent1["agent"]["name"].as_str().unwrap().to_string();
    wait_agent_completed(&mut client, &agent1_name).await;

    // Poll until candidate 1 is genuinely `running_gates` (the `verify`
    // check's `sleep 0.6` is what makes that window real), so the kill
    // below is proven to land mid-gate, not merely mid-queue.
    let mut mid_gate = false;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let entries = queue_entries(&mut client, &repo_name).await;
        if entries
            .iter()
            .any(|e| e["payload"]["status"] == "running_gates")
        {
            mid_gate = true;
            break;
        }
    }
    assert!(
        mid_gate,
        "candidate 1 never reached running_gates before candidate 2 was queued behind it"
    );

    // Explicit gate-start barrier: `running_gates` is a `Space`-level status
    // flag on the queue entry, set by the landing loop before it actually
    // spawns the `verify` check's real child. Confirm the REAL OS process
    // backing it has genuinely started — not just that the flag flipped —
    // by reading back the pid it drops into `shared` (same pid-file
    // technique as this file's `live_verifier_descendant_survives...`
    // test above). This is also the pid the mid-gate kill below must prove
    // is actually dead, not merely detached from, before daemon B starts.
    let verify_pid_path = shared.path().join("verify.pid");
    let verify_pid = wait_for_pid(&verify_pid_path).await;
    assert!(
        process_alive(verify_pid),
        "the verify check's real child must be running before the mid-gate kill below"
    );
    // Captured once here, independent of `OwnedCheckCleanup`'s own copy —
    // this is the identity the post-A-death assertion below re-checks, not
    // a cleanup mechanism.
    let verify_signature = process_signature(verify_pid).expect(
        "the verify check's real child must have an observable start+command signature \
         immediately after it is confirmed alive",
    );
    // Panic-safety net (see `OwnedCheckCleanup` doc comment) for the
    // remainder of this test — a no-op on the successful path. The unique
    // fixture path (not just the "verify.pid" filename every invocation of
    // this test shares) is part of the identity `drop` re-checks.
    let _owned_check_cleanup =
        OwnedCheckCleanup::new(verify_pid, shared.path().display().to_string());

    // Candidate 2: spawned and completed WHILE candidate 1's gate run is
    // still in flight, so its own landing completion enqueues behind
    // candidate 1 on the same `(repo, "main")` FIFO key — `queued`, not
    // `running_gates`, since the lock is held.
    let ticket2 = client
        .call(
            "ticket.new",
            json!({"title": "restart-order candidate 2", "scope": &repo_name}),
        )
        .await
        .unwrap();
    let ticket2_id = ticket2["ticket"]["identity"].as_str().unwrap().to_string();

    let agent2 = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": &ticket2_id,
                "harness": "fake",
                "prompt": "restart candidate note-2",
            }),
        )
        .await
        .unwrap();
    let agent2_name = agent2["agent"]["name"].as_str().unwrap().to_string();
    wait_agent_completed(&mut client, &agent2_name).await;

    let mut both_queued = false;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let entries = queue_entries(&mut client, &repo_name).await;
        if entries.len() == 2 {
            both_queued = true;
            break;
        }
    }
    assert!(
        both_queued,
        "candidate 2's completion must enqueue a second live entry while candidate 1 is \
         still mid-gate"
    );

    // Prove the exact owned check is STILL alive at the moment of the kill
    // below, not merely that it once was: `restart_checks`'s first attempt
    // holds itself open (bounded to a generous 10s) rather than sleeping a
    // fixed duration precisely so this is a guaranteed fact, not a race
    // against however long candidate 2's own spawn/complete/queue steps
    // above happened to take.
    assert!(
        process_alive(verify_pid),
        "the verify check's real child must still be alive at the moment of the kill below — \
         its process_alive-eventually-false later would not prove daemon B recovered an \
         orphan if this process could have exited naturally in between"
    );

    // The kill: a genuine SIGKILL against daemon A's own OS process,
    // observed to completion — `Child::kill` sends the signal AND reaps the
    // process (tokio's own doc: "equivalent to SIGKILL followed by wait"),
    // so this line does not return until daemon A's physical exit is a
    // recorded fact, not an assumption. A real crash runs none of daemon
    // A's own Drop/cascade impls — unlike an in-process task abort, the
    // mid-gate `verify` check's real child (captured above as `verify_pid`,
    // still genuinely blocked per the assertion just above) is left
    // genuinely orphaned, exactly the situation
    // `workflow_exec::reap_stale_managed_children` exists to reclaim.
    let pid_a = child_a
        .id()
        .expect("daemon A must have a pid before the kill");
    child_a.kill().await.expect("SIGKILL daemon A");
    let status = child_a
        .wait()
        .await
        .expect("daemon A's exit status must be observable after the kill");
    assert!(
        !status.success(),
        "a SIGKILLed daemon must not report a successful exit: {status:?}"
    );
    assert!(
        !process_alive(pid_a as i32),
        "daemon A's own OS process must be genuinely dead once reaped"
    );

    // Confirm the captured orphan's identity AND liveness in THIS exact
    // window — after A is physically reaped, before B is even spawned —
    // not just before the kill and again sometime after B starts. Without
    // this, a later process_alive-eventually-false observation could be
    // explained by ordinary post-kill scheduling noise or a coincidence in
    // timing rather than by daemon B's own recovery actually reclaiming a
    // genuine live orphan; this closes that gap directly.
    assert!(
        process_alive(verify_pid),
        "the mid-gate verify check's real process {verify_pid} must still be alive immediately \
         after daemon A is reaped and before daemon B starts — a genuine orphan, not something \
         that already exited on its own"
    );
    assert_eq!(
        process_signature(verify_pid).as_deref(),
        Some(verify_signature.as_str()),
        "the process at pid {verify_pid} immediately after daemon A's death must still be the \
         SAME process (matching start-time+command signature) captured at gate-start — a \
         different identity here would mean the pid was recycled, not that the genuine orphan \
         survived"
    );

    // Both candidates survived the kill, durably queued in FIFO order —
    // proof the kill landed genuinely mid-queue, not after the pipeline had
    // already drained one or both.
    {
        let space = rk_space::Space::open(&layout.db_path()).unwrap();
        let pending = space
            .scan(
                &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                    .identity("landing_queue_entry"),
            )
            .unwrap();
        assert_eq!(pending.len(), 2, "both candidates must survive the kill");
    }

    // Daemon B: a fresh, genuinely independent `rk daemon run` process over
    // the SAME on-disk home — normal startup/recovery, not a test-side
    // shortcut: `Server::run` (server.rs) reclaims the stale pid/socket
    // itself (checking the recorded pid is actually dead, which the
    // assertion above already proved), and
    // `workflow_exec::reap_stale_managed_children` runs before this daemon
    // can serve a single request, verifying its pid+start-time signature
    // before killing anything — never a bare/reused-pid signal.
    // No new agent is ever spawned on daemon B in this test (both
    // candidates already completed before the kill; this restart only
    // replays the durable landing queue), but the same static script is
    // supplied for consistency with daemon A's environment.
    let mut child_b =
        spawn_daemon_process(&layout, &fixture::with_rk_done(&restart_candidates_fake()));
    let mut client = connect(&layout).await;

    // Release barrier: confirm daemon B's own real recovery — not this
    // test — reclaimed the mid-gate check's orphaned real process. By the
    // time `connect` above got a working client, `reap_stale_managed_children`
    // has already run to completion (server.rs calls it, synchronously in
    // program order, before the accept loop that serves any RPC), so this
    // is a short bounded confirmation, not a race against daemon B's own
    // startup.
    let release_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while process_alive(verify_pid) {
        assert!(
            std::time::Instant::now() < release_deadline,
            "the mid-gate verify check's real process {verify_pid} outlived daemon A's crash \
             by more than 5s after daemon B started — its own startup-time \
             reap_stale_managed_children must reclaim daemon-owned background work a real \
             crash orphans"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut drained = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if queue_entries(&mut client, &repo_name).await.is_empty() {
            drained = true;
            break;
        }
    }
    assert!(drained, "the landing queue never drained after the restart");

    // Landing identity: exactly one processed marker per candidate, no
    // duplication from the restart re-running candidate 1's gate.
    let markers = processed_markers(&mut client, &repo_name).await;
    assert_eq!(
        markers.len(),
        2,
        "a restart mid-queue must not duplicate either candidate's landing marker: {markers:?}"
    );
    assert!(
        markers.iter().all(|m| m["payload"]["outcome"] == "landed"),
        "{markers:?}"
    );

    // Order: candidate 1 (queued and gated first) must have landed onto
    // main BEFORE candidate 2 — proven via commit order rather than timing,
    // since the FIFO lock, not wall-clock luck, is what must hold.
    let subjects = git(repo_dir.path(), &["log", "--format=%s", "main"]);
    let subjects: Vec<&str> = subjects.lines().collect();
    let idx1 = subjects
        .iter()
        .position(|s| *s == "docs: add note-1")
        .expect("candidate 1's commit must be on main");
    let idx2 = subjects
        .iter()
        .position(|s| *s == "docs: add note-2")
        .expect("candidate 2's commit must be on main");
    assert!(
        idx2 < idx1,
        "candidate 2 must land AFTER candidate 1 (git log is newest-first, so its subject \
         must appear at a smaller index) — FIFO queue order must survive the restart: \
         {subjects:?}"
    );
    let main_after = git(repo_dir.path(), &["rev-parse", "main"]);
    assert_ne!(main_before, main_after);

    // Ticket ownership: both tickets closed exactly once by the restart's
    // resumed landing, not left open or double-processed.
    for ticket_id in [&ticket1_id, &ticket2_id] {
        let after = client
            .call("ticket.get", json!({"id": ticket_id}))
            .await
            .unwrap();
        assert_eq!(
            after["ticket"]["payload"]["status"], "closed",
            "ticket {ticket_id} must be closed exactly once across the restart: {after}"
        );
    }

    // Budget: both agent records survived the restart durably, with their
    // own distinct declared cost — no duplicate agent record from the
    // restart re-observing either candidate's completion.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let agents = agents["agents"].as_array().unwrap();
    let repo_agents: Vec<_> = agents
        .iter()
        .filter(|a| a["repo_name"].as_str() == Some(repo_name.as_str()))
        .collect();
    assert_eq!(
        repo_agents.len(),
        2,
        "exactly one agent record per candidate must survive the restart, no duplicates: \
         {repo_agents:#?}"
    );
    let total_cost: f64 = repo_agents
        .iter()
        .map(|a| a["cost_usd"].as_f64().unwrap_or(0.0))
        .sum();
    assert!(
        (total_cost - 0.03).abs() < 1e-9,
        "the two candidates' distinct declared costs (0.01 + 0.02) must both be present \
         exactly once: total was {total_cost}"
    );

    // Lease non-leak: a fresh, independent `verify.run` against the same
    // repo's verification admission lane must complete promptly on daemon
    // B — unblocked by any trace of daemon A's aborted in-flight permit
    // (`VerificationAdmission`'s semaphore is in-memory only; a fresh
    // daemon's starts full).
    let post_restart = tokio::time::timeout(
        Duration::from_secs(5),
        run_verify(&layout, &repo_name, "verify"),
    )
    .await
    .expect("a fresh verify.run must not be blocked by any leaked lease from daemon A");
    assert_eq!(post_restart["exit"], json!(0), "{post_restart:#?}");

    let _ = child_b.kill().await;
}
