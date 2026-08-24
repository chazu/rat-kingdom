//! Genuine cross-process daemon-crash proof for the review-ceiling
//! cancellation slice: SIGKILL a real daemon *process* while it is parked
//! **inside** `settle_review_ceiling`, bring a second real daemon up over the
//! same on-disk home, and prove the six properties the feature claims.
//!
//! # Why this is not `live_landing_restart.rs` again
//!
//! That test (and `crates/rk-daemon/tests/*`'s restart tests generally) kills
//! a `tokio::spawn`ed `Daemon::run()` with `handle.abort()` inside the test
//! process, then hand-deletes the pid file and socket the "crash" left
//! behind. That proves a second `LandingPipeline` can be constructed over the
//! same `Space`. It does not exercise the crash-recovery code an operator
//! actually depends on — `Server::run`'s stale-socket reclamation
//! (`crates/rk-daemon/src/server.rs`), which refuses to clobber a socket
//! whose recorded pid is still alive and only reclaims one whose owner is
//! genuinely dead. Here nothing is cleaned up by hand: daemon A is SIGKILLed
//! for real, and daemon B is auto-started by `Client::connect_or_spawn` off
//! the next `rk` invocation exactly as it would be in the field.
//!
//! # Why the crash site is deterministic
//!
//! `settle_review_ceiling` has exactly one non-atomic window: it dismisses
//! the live reviewer (irreversible — a real OS process dies) and only then
//! writes the durable `landing_review_ceiling_settled` marker. Racing that
//! window with a sleep is untestable: on a loaded runner the kill lands
//! before the window opens or long after it closed, and the test stays green
//! either way, proving nothing. So the daemon parks *in* the window instead
//! (`crate::fault` in rk-daemon) and tells us it has, via a file. We kill
//! only after seeing that file, so the crash is guaranteed to land between
//! the dismissal and the durable write on every run.
//!
//! `crash_between_dismissal_and_marker_converges_exactly_once` asserts up
//! front that ZERO settlement markers exist after the crash. That assertion
//! is what keeps the whole test honest: if the barrier ever stopped firing
//! inside the window, that check fails loudly instead of the test quietly
//! degrading into a proof of ordinary restart.

use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// Serializes the four `#[test]`s in this file (never any test in another
/// binary — cargo already runs test binaries one at a time, only the tests
/// *within* one binary run concurrently by default). Each test here starts
/// one or more real daemon processes, each with its own reviewer/lander/
/// canceller subprocesses; four of those fixtures competing for CPU and
/// forks at once is enough self-inflicted contention to occasionally blow
/// this file's deterministic 60-second `until` bounds (reproduced by running
/// two copies of this binary concurrently, doubling that contention: the
/// same properties this file proves eventually held, just past 60s).
/// Serializing removes the contention these tests create for each other
/// without weakening what any single one proves or touching any other
/// binary's concurrency.
static TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Acquire [`TEST_SERIAL`] for the calling test's whole body. Recovers from
/// poisoning: one test panicking while holding the lock must not also fail
/// every test after it.
fn serialize_test() -> std::sync::MutexGuard<'static, ()> {
    TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Must match `BARRIER_CEILING_PRE_MARKER` in `crates/rk-daemon/src/landing.rs`
/// (the constant is private to that module; the barrier's contract is the
/// name string, deliberately, so arming needs no API surface).
const BARRIER_PRE_MARKER: &str = "review-ceiling-pre-marker";
/// Must match `BARRIER_CEILING_POST_MARKER` in `crates/rk-daemon/src/landing.rs`.
const BARRIER_POST_MARKER: &str = "review-ceiling-post-marker";

/// The review workflow the landing pipeline dispatches by name
/// (`landing.rs`'s `REVIEW_WORKFLOW`). Its reviewer hangs rather than
/// returning a verdict, so the candidate stays genuinely `awaiting_review`
/// with a real child process behind it for the whole test.
const REVIEW_WORKFLOW: &str = r#"
package workflow

workflow: {
	name: "steward-review"
	params: {
		taskId:        {type: "string", required: false, default: "unknown"}
		branch:        {type: "string", required: true}
		repo:          {type: "string", required: false, default: "rat-kingdom"}
		target:        {type: "string", required: false, default: "main"}
		headSha:       {type: "string", required: false, default: ""}
		reviewTimeout: {type: "string", required: false, default: "30m"}
	}
	agents: {
		default: {harness: "fake", model: "sonnet"}
	}
	steps: [
		{
			type:   "spawn"
			role:   "reviewer"
			branch: _input.branch
			task: {title: "review", description: "review it"}
		},
		{type: "wait", timeout: _input.reviewTimeout},
		{type: "evaluate", expect: {is_error: false}},
	]
}
"#;

/// Same review workflow, but the reviewer role runs a REAL adapter
/// (`crates/rk-harness/src/{claude,codex}.rs`) instead of `fake`. `fake`
/// parses stdout with the same parser as `claude` but never wraps its
/// session in `crate::watch_pre_work_transport_failure` (see
/// `crates/rk-harness/src/fake.rs`), so a `fake` reviewer can hang or crash
/// but can never produce a `HarnessEvent::TransportFailure` — only a real
/// adapter can exercise the typed-outage path this file's other tests never
/// touch. `harness` is `"claude"` or `"codex"`.
fn review_workflow_real_adapter(harness: &str) -> String {
    format!(
        r#"
package workflow

workflow: {{
	name: "steward-review"
	params: {{
		taskId:        {{type: "string", required: false, default: "unknown"}}
		branch:        {{type: "string", required: true}}
		repo:          {{type: "string", required: false, default: "rat-kingdom"}}
		target:        {{type: "string", required: false, default: "main"}}
		headSha:       {{type: "string", required: false, default: ""}}
		reviewTimeout: {{type: "string", required: false, default: "30m"}}
	}}
	agents: {{
		default: {{harness: "{harness}", model: "sonnet"}}
	}}
	steps: [
		{{
			type:   "spawn"
			role:   "reviewer"
			branch: _input.branch
			task: {{title: "review", description: "review it"}}
		}},
		{{type: "wait", timeout: _input.reviewTimeout}},
		{{type: "evaluate", expect: {{is_error: false}}}},
	]
}}
"#
    )
}

/// Repository policy disabling unattended review-death retry
/// (`LandingPolicy::review_death_auto_retry`, `crates/rk-workflow/src/lib.rs`)
/// so a dead reviewer is fenced by exactly ONE bounded human escalation
/// instead of a 30s-backoff replacement chain — the fastest, most
/// deterministic way to prove convergence without rebuilding the
/// review-death retry matrix (a separate ticket's concern).
const NO_REVIEW_DEATH_RETRY_POLICY: &str =
    "repo: {\n\tlanding: {\n\t\treviewDeathAutoRetry: false\n\t}\n}\n";

/// Fixed, non-retryable transport-classified stderr line — literally the
/// same fixture `rk_harness::transport::classify` and
/// `crates/rk-harness/src/claude.rs`'s own
/// `pre_work_authentication_failure_is_classified_as_not_retryable` test use,
/// reused here instead of inventing a new one.
const TRANSPORT_AUTH_STDERR: &str = "401 Unauthorized: invalid api key";

/// An `rk` invocation driven as the operator, with the fake harness pinned to
/// a hang. The env set here reaches the daemon too: `connect_or_spawn`'s
/// `spawn_detached_daemon` inherits this process's environment, so whichever
/// `rk` call happens to auto-start a daemon hands it `RK_FAKE_HARNESS_CMD`.
fn rk(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
    cmd.env("RK_FAKE_HARNESS_CMD", "sleep 120");
    cmd.env_remove("RK_AGENT");
    cmd.env_remove("RK_AUTH_TOKEN");
    cmd
}

/// Same shape as [`rk`], but hands the auto-started daemon `bin_env`
/// (`"RK_CLAUDE_BIN"` or `"RK_CODEX_BIN"`) instead —
/// `ClaudeHarness`/`CodexHarness::launch` (`crates/rk-harness/src/{claude,codex}.rs`)
/// each read their own var as a fallback binary path exactly like their own
/// `run_fake` unit-test fixtures do.
fn rk_with_bin(home: &Path, bin_env: &str, bin_path: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
    cmd.env(bin_env, bin_path);
    cmd.env_remove("RK_AGENT");
    cmd.env_remove("RK_AUTH_TOKEN");
    cmd
}

/// Write an executable fake adapter binary that fails before ever emitting a
/// parseable started/init event (so it fails pre-`Started`, the only window
/// `crate::watch_pre_work_transport_failure` classifies) with
/// [`TRANSPORT_AUTH_STDERR`] on stderr, then exits non-zero. Mirrors
/// `crates/rk-harness/src/{claude,codex}.rs`'s own `run_fake` test fixture
/// shape — both adapters spawn the binary directly (no shell wrapping), and
/// neither reads its args before failing, so one script body serves both.
fn fake_transport_failure_binary(dir: &Path, name: &str) -> std::path::PathBuf {
    let binary = dir.join(name);
    std::fs::write(
        &binary,
        format!("#!/bin/sh\necho '{TRANSPORT_AUTH_STDERR}' >&2\nexit 1\n"),
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    binary
}

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

fn json_stdout(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "rk failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "bad json: {e}\nstdout: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Poll `attempt` until it yields `Some`, or panic with `what` after 60s.
/// Every wait in this test is a wait on an *observed condition* — never a
/// bare sleep standing in for one.
fn until<T>(what: &str, mut attempt: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        if let Some(v) = attempt() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out after 60s waiting for: {what}");
}

/// A daemon home with `workflow_cue` installed as the review workflow and
/// the disk-pressure floor disabled (a constrained CI temp filesystem would
/// otherwise refuse every spawn before this test reaches anything it means
/// to cover — same reasoning as `daemon_rollover.rs`).
///
/// `supervisor_interval_secs`, when `Some`, overrides `[supervisor]
/// interval_secs` (default 60s — `SupervisorConfig::default`,
/// `crates/rk-core/src/config.rs`): the sweep loop consumes its immediate
/// first tick on startup (`crates/rk-daemon/src/server.rs`) and only then
/// starts ticking on this cadence, so at the default interval a transport-
/// outage episode's own ceiling (`transport_retry_sweep`) can take up to two
/// full intervals to escalate — 120s, well past any reasonable test bound.
fn daemon_home(workflow_cue: &str, supervisor_interval_secs: Option<u64>) -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    let mut config = "[disk]\nmin_free_gb = 0\n\n[harness]\ndefault = \"fake\"\n".to_string();
    if let Some(secs) = supervisor_interval_secs {
        config.push_str(&format!("\n[supervisor]\ninterval_secs = {secs}\n"));
    }
    std::fs::write(home.path().join("config.toml"), config).unwrap();
    let workflows = home.path().join("workflows");
    std::fs::create_dir_all(&workflows).unwrap();
    std::fs::write(workflows.join("steward-review.cue"), workflow_cue).unwrap();
    home
}

/// A repo whose `feature` branch carries a genuinely non-trivial change:
/// `classify_diff` (`crates/rk-daemon/src/supervisor.rs`) only requires review
/// past its trivial threshold, so a smaller diff would land straight through
/// on a gate pass and never reach the review phase this test is about.
///
/// `repo_policy_cue`, when `Some`, is committed as `.rk/repo.cue` alongside
/// the checks — e.g. [`NO_REVIEW_DEATH_RETRY_POLICY`] to fence a dead
/// reviewer with one bounded escalation instead of a retry chain.
fn candidate_repo(repo_policy_cue: Option<&str>) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-b", "main"]);
    git(dir.path(), &["config", "user.email", "r@x"]);
    git(dir.path(), &["config", "user.name", "R"]);
    std::fs::write(dir.path().join("README.md"), "# x\n").unwrap();
    git(dir.path(), &["add", "."]);
    git(dir.path(), &["commit", "-m", "init"]);

    std::fs::create_dir_all(dir.path().join(".rk")).unwrap();
    std::fs::write(
        dir.path().join(".rk/checks.cue"),
        "checks: [\n    {name: \"steward-protected-paths\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"steward-diff-scope\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"verify\", command: \"true\", timeout: \"30s\"},\n]\n",
    )
    .unwrap();
    git(dir.path(), &["add", ".rk/checks.cue"]);
    if let Some(policy) = repo_policy_cue {
        std::fs::write(dir.path().join(".rk/repo.cue"), policy).unwrap();
        git(dir.path(), &["add", ".rk/repo.cue"]);
    }
    git(
        dir.path(),
        &["commit", "-m", "test: register landing checks"],
    );

    git(dir.path(), &["checkout", "-b", "feature"]);
    let body: String = (0..80)
        .map(|n| format!("pub const LINE_{n}: u32 = {n};\n"))
        .collect();
    std::fs::write(dir.path().join("src_gen.rs"), body).unwrap();
    git(dir.path(), &["add", "src_gen.rs"]);
    git(
        dir.path(),
        &["commit", "-m", "feat: add generated constants"],
    );
    let head_sha = git(dir.path(), &["rev-parse", "HEAD"]);
    git(dir.path(), &["checkout", "main"]);
    (dir, head_sha)
}

/// Everything the crash needs to be reproducible, shared by both barrier
/// windows: a live daemon carrying a candidate parked in `awaiting_review`
/// behind a real hung reviewer, with a `cancel-review` already in flight and
/// the daemon confirmed parked at `barrier`.
struct Crashed {
    home: tempfile::TempDir,
    repo: tempfile::TempDir,
    repo_name: String,
    head_sha: String,
    task: String,
    attempt: String,
    reviewer_pid: Option<u32>,
    dead_daemon_pid: u32,
}

impl Drop for Crashed {
    fn drop(&mut self) {
        // The replacement daemon is a real detached process. Stop it on both
        // success and panic so a failed acceptance run cannot leak test
        // daemons into the host running the suite. Do not ask it to shut down
        // gracefully: this fixture deliberately leaves a replacement reviewer
        // waiting, so graceful stop can block behind the behavior under test.
        kill_owning_daemon(self.home.path(), Some(self.dead_daemon_pid));
    }
}

/// Kill whichever daemon currently owns `home`, unless it is this test
/// process itself or `spare` (a pid this test already knows is dead and has
/// no reason to signal again).
///
/// Reads `home`'s pid file directly rather than round-tripping through an
/// `rk daemon status` RPC (as [`daemon_pid`] does for the assertions that are
/// actually under test): teardown runs under exactly the load that can make
/// a subprocess spawn or RPC connect flaky, and a dropped daemon-status call
/// must never silently leave a live daemon behind — unlike every other
/// process this file exercises, a daemon does not exit on its own, so a
/// missed kill here is a permanent leak, not a bounded one. Best-effort: a
/// stale or unreadable pid file (or a `kill` that itself fails to spawn) just
/// means nothing gets signalled, same as today's fallback.
fn kill_owning_daemon(home: &Path, spare: Option<u32>) {
    let Some(pid) = std::fs::read_to_string(home.join("rk.pid"))
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
    else {
        return;
    };
    if pid == std::process::id() || Some(pid) == spare {
        return;
    }
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
}

/// RAII teardown for a test that starts a real detached daemon but — unlike
/// the crash tests above — never crashes it itself, so nothing else in the
/// test tears it down. Declared *after* the `TempDir` it guards so it drops
/// *before* that `TempDir`'s own destructor removes the directory (Rust
/// drops locals in reverse declaration order): the pid file must still exist
/// when [`kill_owning_daemon`] reads it. Runs on both success and panic,
/// same as [`Crashed`]'s own cleanup.
struct DaemonGuard {
    home: std::path::PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        kill_owning_daemon(&self.home, None);
    }
}

fn tuples(home: &Path, scope: &str, category: &str, identity: &str) -> Vec<Value> {
    let out = rk(home)
        .args(["--json", "scan", category, scope, identity])
        .output()
        .unwrap();
    json_stdout(&out)["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Settlement markers for one specific attempt — the count that must be
/// exactly one after convergence. Filtered by attempt on purpose: a count
/// over all attempts would be satisfied by a marker for the *re-enqueued*
/// attempt and would silently stop testing what it names.
fn settlements(home: &Path, repo_name: &str, attempt: &str) -> Vec<Value> {
    tuples(home, repo_name, "event", "landing_review_ceiling_settled")
        .into_iter()
        .filter(|t| t["payload"]["attempt"].as_str() == Some(attempt))
        .collect()
}

/// The pid of the daemon currently owning `home`, or `None` if none is up.
///
/// `rk daemon status` is a pure status read: it reports on a daemon, it does
/// not start one. Bringing a daemon UP is what `Client::connect_or_spawn`
/// does, on the ordinary RPC commands — hence [`start_daemon`].
fn daemon_pid(home: &Path) -> Option<u32> {
    let out = rk(home)
        .args(["--json", "daemon", "status"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    serde_json::from_slice::<Value>(&out.stdout).ok()?["pid"]
        .as_u64()
        .map(|p| p as u32)
}

/// Bring a daemon up over `home` the way the field does — by making an
/// ordinary RPC call and letting `Client::connect_or_spawn` auto-start one —
/// and return its pid. After a SIGKILL this is the path that exercises
/// `Server::run`'s stale-socket reclamation.
fn start_daemon(home: &Path) -> u32 {
    until("a daemon to come up over the home", || {
        // `rk list` is a plain `agent.list` RPC through `connect_or_spawn`.
        let out = rk(home).args(["--json", "list"]).output().ok()?;
        out.status.success().then_some(())
    });
    until("the freshly started daemon to report its pid", || {
        daemon_pid(home)
    })
}

/// Live (non-terminal) agents belonging to `attempt`'s workflow instance —
/// the orphan check. `Dismissed`/`Failed`/`Completed` records legitimately
/// persist; a *running* one after settlement is the leak.
fn live_agents_for(home: &Path, attempt: &str) -> Vec<Value> {
    let out = rk(home).args(["--json", "list"]).output().unwrap();
    json_stdout(&out)
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|a| {
            a["workflow_instance"].as_str() == Some(attempt)
                && matches!(a["state"].as_str(), Some("running" | "spawning" | "parked"))
        })
        .collect()
}

/// Drive a candidate to `awaiting_review` behind a real hung reviewer, arm
/// `barrier`, fire `rk cancel-review`, wait for the daemon to actually park
/// inside the barrier, then SIGKILL it. Returns once the daemon process is
/// confirmed dead and the barrier is disarmed for its successor.
fn crash_at(barrier: &str) -> Crashed {
    let home = daemon_home(REVIEW_WORKFLOW, None);
    let (repo, head_sha) = candidate_repo(None);
    let repo_name = repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let home_path = home.path().to_path_buf();

    // Arm BEFORE anything can reach `settle_review_ceiling`. The only other
    // caller is the ceiling timeout, 30m out per this workflow's
    // `reviewTimeout`, so arming this early cannot fire early.
    std::fs::write(home_path.join("fault-barrier"), barrier).unwrap();

    // The first `rk` call auto-starts daemon A.
    json_stdout(
        &rk(&home_path)
            .args(["--json", "repo", "add", repo.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );
    let task = json_stdout(
        &rk(&home_path)
            .args([
                "--json",
                "ticket",
                "new",
                "add generated constants",
                "--repo",
                &repo_name,
            ])
            .output()
            .unwrap(),
    )["identity"]
        .as_str()
        .unwrap()
        .to_string();

    // `rk land` does not return until the whole landing decision settles —
    // including the review wait, here a deliberate hang — so it runs on its
    // own process and is never awaited. It dies with daemon A; that is part
    // of what daemon B has to converge out of.
    let mut lander = rk(&home_path)
        .args([
            "land",
            "feature",
            "--repo",
            repo.path().to_str().unwrap(),
            "--target",
            "main",
            "--task",
            &task,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    until("the candidate to reach awaiting_review", || {
        tuples(&home_path, &repo_name, "event", "landing_queue_entry")
            .into_iter()
            .find(|t| t["payload"]["status"] == "awaiting_review")
    });

    // The attempt id is a deterministic hash of repo/branch/head/target/task
    // that `rk-daemon` does not export; read it back off the reviewer's own
    // workflow-instance binding instead of recomputing it.
    let reviewer = until("the reviewer agent to spawn", || {
        let out = rk(&home_path).args(["--json", "list"]).output().unwrap();
        json_stdout(&out)
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .find(|a| a["role"].as_str() == Some("reviewer"))
    });
    let attempt = reviewer["workflow_instance"]
        .as_str()
        .expect("a review-workflow-owned agent must carry its instance id")
        .to_string();
    let reviewer_pid = reviewer["pid"].as_u64().map(|p| p as u32);

    let daemon_pid = daemon_pid(&home_path).expect("daemon A must report a real pid");
    assert_ne!(
        daemon_pid,
        std::process::id(),
        "refusing to SIGKILL this test process"
    );

    // Fire the cancel and leave it in flight: the daemon parks inside
    // `settle_review_ceiling`, so this process will never return.
    let mut canceller = rk(&home_path)
        .args([
            "cancel-review",
            "feature",
            "--repo",
            repo.path().to_str().unwrap(),
            "--target",
            "main",
            "--task",
            &task,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // THE deterministic point. Not a duration — the daemon telling us it is
    // parked inside the transition.
    let reached = home_path.join("fault-barrier.reached");
    until("the daemon to park inside the fault barrier", || {
        std::fs::read_to_string(&reached).ok()
    });
    assert!(
        process_alive(daemon_pid),
        "daemon {daemon_pid} must still be alive at the barrier — otherwise the kill below \
         is not what crashed it"
    );

    // A real SIGKILL of a real daemon process: no Drop, no graceful
    // shutdown, no hand-cleanup of the pid file or socket it leaves behind.
    Command::new("kill")
        .args(["-9", &daemon_pid.to_string()])
        .status()
        .unwrap();
    until("daemon A to actually die", || {
        (!process_alive(daemon_pid)).then_some(())
    });

    // Disarm, so daemon B comes up able to complete the same transition.
    std::fs::remove_file(home_path.join("fault-barrier")).ok();
    std::fs::remove_file(&reached).ok();
    let _ = lander.kill();
    let _ = lander.wait();
    let _ = canceller.kill();
    let _ = canceller.wait();

    Crashed {
        home,
        repo,
        repo_name,
        head_sha,
        task,
        attempt,
        reviewer_pid,
        dead_daemon_pid: daemon_pid,
    }
}

fn cancel_review(c: &Crashed) -> std::process::Output {
    rk(c.home.path())
        .args([
            "cancel-review",
            "feature",
            "--repo",
            c.repo.path().to_str().unwrap(),
            "--target",
            "main",
            "--task",
            &c.task,
        ])
        .output()
        .unwrap()
}

fn reenqueue(c: &Crashed) -> std::process::Output {
    rk(c.home.path())
        .args([
            "--json",
            "reenqueue-review",
            "feature",
            "--repo",
            c.repo.path().to_str().unwrap(),
            "--target",
            "main",
            "--task",
            &c.task,
            "--attempt",
            &c.attempt,
        ])
        .output()
        .unwrap()
}

/// Every post-restart property the feature owes an operator, proven against
/// daemon B after a crash in the pre-marker window: the reviewer was already
/// killed, but nothing durable recorded it.
#[test]
fn crash_between_dismissal_and_marker_converges_exactly_once() {
    let _serial = serialize_test();
    let c = crash_at(BARRIER_PRE_MARKER);
    let home = c.home.path();

    // Daemon B: auto-started by this very call through
    // `Client::connect_or_spawn`, reclaiming the stale socket and pid file
    // SIGKILL left behind. Nothing in this test removed them.
    let restarted_pid = start_daemon(home);
    assert_ne!(
        restarted_pid, c.dead_daemon_pid,
        "daemon B must be a genuinely new process, not the one we killed"
    );

    // Honesty check for the whole test (see module doc): the crash really did
    // land BEFORE the durable write. If the barrier ever stops firing inside
    // the window, this fails loudly instead of the test quietly becoming a
    // proof of ordinary restart.
    assert!(
        settlements(home, &c.repo_name, &c.attempt).is_empty(),
        "the pre-marker barrier must have crashed the daemon before the settlement was durable"
    );

    // No orphan: the dismissal that DID happen before the crash killed a real
    // OS process, and it stays dead across the restart.
    if let Some(pid) = c.reviewer_pid {
        until("the dismissed reviewer's OS process to be gone", || {
            (!process_alive(pid)).then_some(())
        });
    }

    // Exactly-once convergence: the operator retries the cancel their crashed
    // one never completed, and it succeeds, leaving exactly ONE marker.
    let retry = cancel_review(&c);
    assert!(
        retry.status.success(),
        "the interrupted cancel must be completable after the restart: {}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let settled = settlements(home, &c.repo_name, &c.attempt);
    assert_eq!(
        settled.len(),
        1,
        "convergence must leave exactly one settlement marker: {settled:?}"
    );
    assert_eq!(settled[0]["payload"]["reason"], "operator-cancelled");
    assert_eq!(settled[0]["payload"]["head_sha"], c.head_sha);

    // No duplicate reviewer: settlement fenced the attempt, so nothing may be
    // running under it. Polled, not a single snapshot: the dismissed
    // reviewer's `AgentRecord` converges to a terminal state via the
    // daemon's own async reconciliation of the OS-process kill above, not
    // synchronously with the RPC that requested it, so a snapshot taken
    // immediately can catch it a beat before that reconciliation lands
    // (worse, and more likely to actually flip the result, under the CPU
    // contention `mise run verify`'s ordinary in-binary test concurrency
    // creates).
    until("no agent to still be live under the settled attempt", || {
        live_agents_for(home, &c.attempt).is_empty().then_some(())
    });

    assert_converged_properties(&c);
}

/// The mirror window: the settlement reached disk, then the daemon died
/// before its caller could be told. The operator's natural retry must be
/// REFUSED rather than settling a second time.
#[test]
fn crash_after_durable_marker_refuses_the_retry_rather_than_settling_twice() {
    let _serial = serialize_test();
    let c = crash_at(BARRIER_POST_MARKER);
    let home = c.home.path();

    let restarted_pid = start_daemon(home);
    assert_ne!(
        restarted_pid, c.dead_daemon_pid,
        "daemon B must be a genuinely new process, not the one we killed"
    );

    // The write survived the SIGKILL — exactly once, with no second daemon
    // and no second call having added to it.
    let settled = settlements(home, &c.repo_name, &c.attempt);
    assert_eq!(
        settled.len(),
        1,
        "the durable settlement must survive the crash exactly once: {settled:?}"
    );
    assert_eq!(settled[0]["payload"]["reason"], "operator-cancelled");

    // Stale-attempt rejection: the operator never saw their cancel succeed,
    // so they retry. It must be refused, not honoured a second time.
    let retry = cancel_review(&c);
    assert!(
        !retry.status.success(),
        "a cancel retry against an already-settled attempt must be refused, not silently \
         accepted: {}",
        String::from_utf8_lossy(&retry.stdout)
    );
    assert_eq!(
        settlements(home, &c.repo_name, &c.attempt).len(),
        1,
        "the refused retry must not have written a second marker"
    );

    assert_converged_properties(&c);
}

/// Properties that must hold once an attempt is settled, whichever window the
/// crash landed in: bounded re-enqueue still works and is still idempotent,
/// and a late verdict from the dead generation is retained as evidence
/// WITHOUT mutating the terminal decision.
fn assert_converged_properties(c: &Crashed) {
    let home = c.home.path();

    // A late APPROVE from the CANCELLED generation, written exactly as a
    // zombie reviewer finishing its in-flight turn after the kill would.
    let payload = serde_json::json!({
        "task": c.task,
        "recommendation": "APPROVE",
        "notes": "arrived after the crash",
        "head_sha": c.head_sha,
        "branch": "feature",
        "target": "main",
        "review_attempt": c.attempt,
    })
    .to_string();
    let out = rk(home)
        .args([
            "out",
            "artifact",
            &c.repo_name,
            "review",
            "--payload",
            &payload,
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "writing the late verdict: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // It is retained as EVIDENCE by daemon B's own periodic reconciliation...
    until("the late verdict to be retained as evidence", || {
        tuples(
            home,
            &c.repo_name,
            "artifact",
            "landing_late_review_evidence",
        )
        .into_iter()
        .find(|t| t["payload"]["attempt"].as_str() == Some(c.attempt.as_str()))
    });

    // ...and changes nothing: the decision stays terminal and the cancelled
    // candidate never lands.
    assert_eq!(
        settlements(home, &c.repo_name, &c.attempt).len(),
        1,
        "late evidence must not add or replace a settlement marker"
    );
    let main_head = git(c.repo.path(), &["rev-parse", "main"]);
    assert_ne!(
        main_head, c.head_sha,
        "an APPROVE from a cancelled generation must never land the branch"
    );

    // Prove re-enqueue only after the late-evidence sweep has run. A fresh
    // reviewer deliberately hangs in this fixture; dispatching it first would
    // put the single landing loop back into `await_primary_verdict` and prevent
    // that same loop from reaching its reconciliation step until the new wait
    // ended. That would test scheduler ordering, not evidence durability.
    let out = reenqueue(c);
    assert!(
        out.status.success(),
        "re-enqueue must work against the restarted daemon: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let new_attempt = json_stdout(&out)["new_attempt"]
        .as_str()
        .expect("re-enqueue must report the fresh attempt id")
        .to_string();
    assert_ne!(
        new_attempt, c.attempt,
        "the replacement must be a new attempt"
    );

    // Idempotent: a second call for the same settled attempt returns the SAME
    // replacement rather than dispatching another reviewer.
    let repeat = reenqueue(c);
    assert!(repeat.status.success());
    assert_eq!(
        json_stdout(&repeat)["new_attempt"].as_str(),
        Some(new_attempt.as_str()),
        "a repeat re-enqueue must return the same attempt, never dispatch a duplicate"
    );
    // Same eventual-convergence property as the first `live_agents_for`
    // check above (see its comment): polled rather than snapshotted once.
    until("re-enqueue to never revive the settled attempt", || {
        live_agents_for(home, &c.attempt).is_empty().then_some(())
    });
}

/// The gap the other tests in this file never cover: every scenario above
/// pins the reviewer to the `fake` harness's `sleep 120` — a genuinely LIVE,
/// hung process, so nothing here ever exercises
/// `rk_harness::transport::classify` at all (`fake` never wraps its session
/// in `watch_pre_work_transport_failure` — see
/// [`review_workflow_real_adapter`]'s doc). This proves the SAME
/// review-workflow fixture, driven with a REAL adapter (`claude` or `codex`)
/// against a reviewer whose stderr classifies as a non-retryable transport
/// outage instead of hanging: the operator sees a TYPED `transport-outage`
/// row rather than the undifferentiated `agent-failed` row a plain crash
/// gets (`crates/rk-daemon/src/inbox.rs`'s `transport_outage_item` — "never
/// BOTH", it replaces the generic row), and the landing decision still
/// converges to exactly one bounded human escalation rather than hanging
/// behind the dead reviewer — the same "never lands, never hangs forever"
/// property `crash_between_dismissal_and_marker_converges_exactly_once`
/// proves for a live-forever reviewer, just routed through
/// `route_review_death` (a dead reviewer) instead of
/// `settle_review_ceiling` (a still-live one at the wait ceiling).
///
/// `harness`/`bin_env`/`bin_name` select the real adapter under test
/// (`"claude"`/`"RK_CLAUDE_BIN"`/`"claude-fake-outage"` or
/// `"codex"`/`"RK_CODEX_BIN"`/`"codex-fake-outage"`); both wrap their session
/// in `crate::watch_pre_work_transport_failure`
/// (`crates/rk-harness/src/{claude,codex}.rs`) and read the same fixture
/// literal ([`TRANSPORT_AUTH_STDERR`]) in their own crate's unit tests, so
/// one assertion body proves the seam for both instead of trusting it
/// generalizes from one adapter to the other.
///
/// No daemon crash, no restart: that machinery belongs to the tests above
/// and is not this gap's concern.
fn assert_transport_outage_is_typed_and_fenced(harness: &str, bin_env: &str, bin_name: &str) {
    // Fast sweep cadence: the transport-outage retry sweep must reach this
    // non-retryable episode's ceiling well inside the `until` bounds below.
    let home = daemon_home(&review_workflow_real_adapter(harness), Some(1));
    let home_path = home.path().to_path_buf();
    // Unlike the crash tests above, nothing here ever kills this daemon —
    // the whole point of this test is that it converges without a crash —
    // so without this guard it (and its socket) would run forever after the
    // test exits. See `DaemonGuard`'s doc for why it must be declared after
    // `home`.
    let _daemon_guard = DaemonGuard {
        home: home_path.clone(),
    };
    let bin = fake_transport_failure_binary(home.path(), bin_name);

    let (repo, head_sha) = candidate_repo(Some(NO_REVIEW_DEATH_RETRY_POLICY));
    let repo_name = repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    json_stdout(
        &rk_with_bin(&home_path, bin_env, &bin)
            .args(["--json", "repo", "add", repo.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );
    let task = json_stdout(
        &rk_with_bin(&home_path, bin_env, &bin)
            .args([
                "--json",
                "ticket",
                "new",
                "add generated constants",
                "--repo",
                &repo_name,
            ])
            .output()
            .unwrap(),
    )["identity"]
        .as_str()
        .unwrap()
        .to_string();

    // `rk land` does not return until the whole landing decision settles
    // (module doc at the top of this file) — here that is fast (no retry
    // chain to wait out), but run it detached anyway so a regression that
    // DOES make it hang fails on the `until` bounds below rather than
    // wedging the test process.
    let mut lander = rk_with_bin(&home_path, bin_env, &bin)
        .args([
            "land",
            "feature",
            "--repo",
            repo.path().to_str().unwrap(),
            "--target",
            "main",
            "--task",
            &task,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // The reviewer's OS process dies almost immediately (pre-`Started`), but
    // its `AgentRecord` and the typed classification persist for the
    // operator to read. Polled to require BOTH the typed classification AND
    // the terminal state: `TransportFailure` (which records
    // `transport_outage`) and `Exited` (which drives the ordinary
    // live->Failed transition) are two separate events, so a snapshot can
    // briefly observe the classification on a still-`running` record.
    let reviewer = until(
        "the reviewer agent to record a transport outage and go terminal",
        || {
            let out = rk(&home_path).args(["--json", "list"]).output().ok()?;
            json_stdout(&out)
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .find(|a| {
                    a["role"].as_str() == Some("reviewer")
                        && !a["transport_outage"].is_null()
                        && a["state"].as_str() == Some("failed")
                })
        },
    );
    assert_eq!(reviewer["transport_outage"]["provider"], harness);
    assert_eq!(reviewer["transport_outage"]["class"], "authentication");
    assert_eq!(
        reviewer["transport_outage"]["retryable"], false,
        "a rejected credential does not heal by reconnecting"
    );
    let reviewer_name = reviewer["name"]
        .as_str()
        .expect("agent record must carry its own name")
        .to_string();

    // Visible as a TYPED outage, never the undifferentiated stuck/failed row
    // a plain crash or hang gets.
    let inbox_item = until(
        "the inbox to carry a typed transport-outage row for the reviewer",
        || {
            let out = rk(&home_path).args(["--json", "inbox"]).output().ok()?;
            let items: Vec<Value> = serde_json::from_slice(&out.stdout).ok()?;
            items
                .into_iter()
                .find(|it| it["subject"].as_str() == Some(reviewer_name.as_str()))
        },
    );
    assert_eq!(
        inbox_item["kind"], "transport-outage",
        "a reviewer mid a typed transport-outage episode must not surface as the generic \
         agent-failed row: {inbox_item:?}"
    );

    // Fenced: the landing decision does not hang behind the dead reviewer —
    // with unattended review-death retry disabled by policy, it converges to
    // exactly ONE bounded human escalation.
    let need = until(
        "the review-death escalation to land as a steward need",
        || {
            tuples(&home_path, &repo_name, "need", "steward")
                .into_iter()
                .find(|t| {
                    t["payload"]["text"]
                        .as_str()
                        .is_some_and(|s| s.contains("died before a verdict"))
                })
        },
    );
    assert!(
        need["payload"]["text"]
            .as_str()
            .unwrap()
            .contains(task.as_str()),
        "the escalation must name the exact task it is holding: {need:?}"
    );

    // `rk land` itself returns — never hangs — once the decision is
    // escalated.
    let status = until("the detached `rk land` to exit", || {
        lander.try_wait().ok().flatten()
    });
    assert!(
        status.success(),
        "an escalated-but-resolved landing decision is not a CLI failure"
    );

    // Never lands a candidate whose reviewer died before a verdict.
    let main_head = git(repo.path(), &["rev-parse", "main"]);
    assert_ne!(
        main_head, head_sha,
        "a candidate whose reviewer died before a verdict must never land"
    );
}

#[test]
fn transport_classified_claude_reviewer_death_is_a_typed_outage_not_a_plain_hang() {
    let _serial = serialize_test();
    assert_transport_outage_is_typed_and_fenced("claude", "RK_CLAUDE_BIN", "claude-fake-outage");
}

#[test]
fn transport_classified_codex_reviewer_death_is_a_typed_outage_not_a_plain_hang() {
    let _serial = serialize_test();
    assert_transport_outage_is_typed_and_fenced("codex", "RK_CODEX_BIN", "codex-fake-outage");
}
