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
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

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

/// A daemon home with the review workflow installed and the disk-pressure
/// floor disabled (a constrained CI temp filesystem would otherwise refuse
/// every spawn before this test reaches anything it means to cover — same
/// reasoning as `daemon_rollover.rs`).
fn daemon_home() -> tempfile::TempDir {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        "[disk]\nmin_free_gb = 0\n\n[harness]\ndefault = \"fake\"\n",
    )
    .unwrap();
    let workflows = home.path().join("workflows");
    std::fs::create_dir_all(&workflows).unwrap();
    std::fs::write(workflows.join("steward-review.cue"), REVIEW_WORKFLOW).unwrap();
    home
}

/// A repo whose `feature` branch carries a genuinely non-trivial change:
/// `classify_diff` (`crates/rk-daemon/src/supervisor.rs`) only requires review
/// past its trivial threshold, so a smaller diff would land straight through
/// on a gate pass and never reach the review phase this test is about.
fn candidate_repo() -> (tempfile::TempDir, String) {
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
    git(dir.path(), &["commit", "-m", "test: register landing checks"]);

    git(dir.path(), &["checkout", "-b", "feature"]);
    let body: String = (0..80)
        .map(|n| format!("pub const LINE_{n}: u32 = {n};\n"))
        .collect();
    std::fs::write(dir.path().join("src_gen.rs"), body).unwrap();
    git(dir.path(), &["add", "src_gen.rs"]);
    git(dir.path(), &["commit", "-m", "feat: add generated constants"]);
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
                && matches!(
                    a["state"].as_str(),
                    Some("running" | "spawning" | "parked")
                )
        })
        .collect()
}

/// Drive a candidate to `awaiting_review` behind a real hung reviewer, arm
/// `barrier`, fire `rk cancel-review`, wait for the daemon to actually park
/// inside the barrier, then SIGKILL it. Returns once the daemon process is
/// confirmed dead and the barrier is disarmed for its successor.
fn crash_at(barrier: &str) -> Crashed {
    let home = daemon_home();
    let (repo, head_sha) = candidate_repo();
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

    let daemon_pid = json_stdout(
        &rk(&home_path)
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .expect("daemon status must report a real pid") as u32;

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
    let c = crash_at(BARRIER_PRE_MARKER);
    let home = c.home.path();

    // Daemon B: auto-started by this very call through
    // `Client::connect_or_spawn`, reclaiming the stale socket and pid file
    // SIGKILL left behind. Nothing in this test removed them.
    let restarted_pid = until("daemon B to come up over the same home", || {
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
    });
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
    // running under it.
    assert!(
        live_agents_for(home, &c.attempt).is_empty(),
        "no agent may still be live under a settled attempt"
    );

    assert_converged_properties(&c);
}

/// The mirror window: the settlement reached disk, then the daemon died
/// before its caller could be told. The operator's natural retry must be
/// REFUSED rather than settling a second time.
#[test]
fn crash_after_durable_marker_refuses_the_retry_rather_than_settling_twice() {
    let c = crash_at(BARRIER_POST_MARKER);
    let home = c.home.path();

    until("daemon B to come up over the same home", || {
        let out = rk(home)
            .args(["--json", "daemon", "status"])
            .output()
            .ok()?;
        out.status.success().then_some(())
    });

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

    // Bounded re-enqueue survives the restart: one fresh attempt, dispatched
    // once.
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
    assert_ne!(new_attempt, c.attempt, "the replacement must be a new attempt");

    // Idempotent: a second call for the same settled attempt returns the SAME
    // replacement rather than dispatching another reviewer.
    let repeat = reenqueue(c);
    assert!(repeat.status.success());
    assert_eq!(
        json_stdout(&repeat)["new_attempt"].as_str(),
        Some(new_attempt.as_str()),
        "a repeat re-enqueue must return the same attempt, never dispatch a duplicate"
    );
    assert!(
        live_agents_for(home, &c.attempt).is_empty(),
        "re-enqueue must never revive the settled attempt"
    );

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
        tuples(home, &c.repo_name, "artifact", "landing_late_review_evidence")
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
}
