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
fn write_saturation_checks(repo: &Path, shared: &Path, n: usize) {
    let mut checks = String::from("checks: [\n");
    for i in 0..n {
        let fail = i == n - 1;
        let body = marker_check_body(shared, fail);
        // `sharedCargoTarget: true` is what actually routes a check through
        // the per-repo verification admission queue at all
        // (`workflow_exec.rs::run_check_in`: `admission_limit` is 0 —
        // disabled — for any check that doesn't opt in). This mirrors the
        // repo's own real `.rk/checks.cue` `verify` check, the one entry
        // this whole ticket's admission queue exists to bound.
        checks.push_str(&format!(
            "    {{name: \"sat-{i}\", command: \"{}\", timeout: \"10s\", environmentPolicy: \"strip_rk_spawn\", sharedCargoTarget: true}},\n",
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
    write_saturation_checks(repo_dir.path(), shared.path(), N_CHECKS);

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
