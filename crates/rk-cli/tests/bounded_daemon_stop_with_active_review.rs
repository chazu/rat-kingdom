//! TKT-karut-jaraf-hivur: genuine cross-*process* proof that a graceful
//! `rk daemon stop` makes the OLD daemon's real OS process exit within a
//! bound while a real reviewer child process is still alive and mid-review,
//! and that the durable candidate state it leaves behind is exactly what it
//! was before the stop — never a fabricated/forced settlement invented just
//! to exit fast.
//!
//! `crates/rk-daemon/tests/bounded_shutdown_with_active_review.rs` proves the
//! logical half of this fix (`LandingPipeline::await_primary_verdict` races
//! its poll against the daemon's shutdown signal) but does so with both
//! "daemons" as `tokio::spawn`ed futures inside one test process — there is
//! no second OS process to exit, so it cannot stand as proof of physical PID
//! exit. This file is that missing half, built the same way
//! `review_ceiling_crash_barrier.rs` proves its own cross-process daemon
//! crash: real `rk` subprocesses, `Client::connect_or_spawn` auto-starting
//! the daemon exactly as the field does, and a real `kill -0`-checked PID.
//!
//! Unlike that file's SIGKILL barrier (which proves crash recovery), this
//! fixture uses an ordinary graceful `rk daemon stop` while a real reviewer
//! subprocess is genuinely alive and blocked on a marker file — the exact
//! shape of the confirmed production incident (one live native reviewer,
//! zero executing checks). It proves two things, and is explicit about a
//! third it does NOT prove, kept honest rather than silently assumed:
//! 1. the OLD daemon's real OS pid actually exits within a bound (not "the
//!    RPC replied", not "a tokio task returned" — `kill -0` on the process
//!    lists this test observed via `rk daemon status`);
//! 2. the durable candidate state a restart resumes from is untouched by
//!    that stop — still an in-flight, resumable status, and no forced
//!    review-ceiling settlement was ever written for it, just to make
//!    shutdown fast;
//! 3. it does NOT prove the reviewer's own OS process survives the stop or
//!    reconnects to the replacement daemon to deliver its own verdict. This
//!    fixture's own comments below the stop record a real, separate finding
//!    (published to BBS, filed as follow-up TKT-rohib-rukaf-sizak): the
//!    pre-existing `Child::kill_on_drop(true)` in `crates/rk-harness/src/lib.rs`
//!    reaps that reviewer's process as an incidental side effect of the
//!    surrounding runtime tearing down, observed on every run during
//!    development — this fix does not touch that at all. What actually
//!    resumes the candidate below is an INJECTED verdict, built from the
//!    review context this test captured off the original daemon's own
//!    binding before the stop, standing in for a real reviewer's report. It
//!    is never presented as, and must never be read as, proof of the
//!    reviewer process itself surviving and reconnecting.
//!
//! What this file does NOT claim, matching the BBS contract published
//! alongside this fix (finding 01M2HTJ67JK3XCMX89KFQWG5A7): it does not
//! prove the pending `submit_manual` detached-background-drain handoff
//! (rat/bristle-16/tkt-dobas-lujom-lipog, still under native review) is
//! joined by `Server::run`'s shutdown — that branch is not landed, and nothing
//! here depends on it. It also does not attempt to bound GATE CHECK
//! subprocess execution against shutdown (`execute_gate_plan_at`) — the
//! confirmed production incident had zero executing checks, only a live
//! reviewer, and that remains this ticket's exact, deliberately narrow scope.

use serde_json::Value;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn rk(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
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

/// Poll `attempt` until it yields `Some`, or panic with `what` after 60s —
/// the setup-phase budget. The one bound this file actually asserts on
/// (the graceful-stop-to-physical-exit window) is measured separately,
/// below, against a much tighter margin.
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

/// Bring a daemon up over `home` the way the field does — an ordinary RPC
/// call through `Client::connect_or_spawn` — and return its pid.
fn start_daemon(home: &Path) -> u32 {
    until("a daemon to come up over the home", || {
        let out = rk(home).args(["--json", "list"]).output().ok()?;
        out.status.success().then_some(())
    });
    until("the freshly started daemon to report its pid", || {
        daemon_pid(home)
    })
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

/// A repo with two unlanded branches, neither ever needing `agent.spawn`
/// (`rk land` submits an existing branch directly): `feature-a`, a
/// genuinely non-trivial change (`classify_diff` in
/// `crates/rk-daemon/src/supervisor.rs` only routes past-trivial diffs
/// through review — same 80-line trick `review_ceiling_crash_barrier.rs`
/// uses), and `feature-b`, doc-only (never needs review).
fn candidate_repo() -> tempfile::TempDir {
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
        "checks: [\n    {name: \"landing-protected-paths\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"landing-diff-scope\", command: \"true\", timeout: \"30s\"},\n    \
         {name: \"verify\", command: \"true\", timeout: \"30s\"},\n]\n",
    )
    .unwrap();
    std::fs::write(dir.path().join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(dir.path(), &["add", ".rk/checks.cue", ".rk/repo.cue"]);
    git(
        dir.path(),
        &["commit", "-m", "test: register landing checks"],
    );

    git(dir.path(), &["checkout", "-b", "feature-a"]);
    let body: String = (0..80)
        .map(|n| format!("pub const LINE_{n}: u32 = {n};\n"))
        .collect();
    std::fs::write(dir.path().join("src_gen.rs"), body).unwrap();
    git(dir.path(), &["add", "src_gen.rs"]);
    git(
        dir.path(),
        &["commit", "-m", "feat: add generated constants"],
    );
    git(dir.path(), &["checkout", "main"]);

    git(dir.path(), &["checkout", "-b", "feature-b"]);
    std::fs::create_dir_all(dir.path().join("docs")).unwrap();
    std::fs::write(dir.path().join("docs/note.md"), "queued second candidate\n").unwrap();
    git(dir.path(), &["add", "docs/note.md"]);
    git(
        dir.path(),
        &["commit", "-m", "docs: second queued candidate"],
    );
    git(dir.path(), &["checkout", "main"]);

    dir
}

const REVIEW_WORKFLOW: &str = r#"
package workflow
workflow: {
    name: "candidate-review"
    params: {
        taskId:        {type: "string", required: false, default: "unknown"}
        branch:        {type: "string", required: true}
        repo:          {type: "string", required: false, default: "rat-kingdom"}
        target:        {type: "string", required: false, default: "main"}
        headSha:       {type: "string", required: false, default: ""}
        reviewTimeout: {type: "string", required: false, default: "10m"}
    }
    agents: {default: {harness: "fake"}}
    steps: [
        {type: "spawn", role: "reviewer", branch: _input.branch,
         task: {title: "review", description: "review it"}},
        {type: "wait", timeout: _input.reviewTimeout},
        {type: "evaluate", expect: {is_error: false}},
    ]
}
"#;

/// The only fake generation this fixture ever spawns is the reviewer —
/// `rk land` submits `feature-a`/`feature-b` directly, no implementer rat
/// needed. Blocks on a marker file this test controls (bounded to ~30s so
/// an orphaned process from a failed assertion still exits on its own),
/// reproducing the "marker-held reviewer" the production incident
/// described, then delivers its verdict through the SAME `rk out artifact`
/// route a real reviewer's LLM turn would — using its own `RK_REVIEW_*`
/// env bindings (`crates/rk-core/src/review.rs`), not a value this test
/// computes or injects on its behalf.
fn reviewer_fake_script(rk_bin: &str) -> String {
    format!(
        r#"
read -r _prompt
i=0
while [ ! -f "$RK_HOME/release-reviewer" ] && [ "$i" -lt 600 ]; do
  sleep 0.05
  i=$((i+1))
done
"{rk_bin}" out artifact "$RK_REPO" review --payload "{{\"task\":\"$RK_REVIEW_TASK\",\"recommendation\":\"APPROVE\",\"notes\":\"resumed after graceful stop\",\"branch\":\"$RK_REVIEW_BRANCH\",\"head_sha\":\"$RK_REVIEW_HEAD\",\"target\":\"$RK_REVIEW_TARGET\",\"review_attempt\":\"$RK_REVIEW_ATTEMPT\"}}" >/dev/null 2>&1
echo '{{"type":"system","subtype":"init","session_id":"marker-reviewer"}}'
"{rk_bin}" done "review complete" >/dev/null 2>&1
echo '{{"type":"result","subtype":"success","is_error":false,"result":"reviewed","session_id":"marker-reviewer","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#
    )
}

fn ref_contains(repo: &Path, rev: &str, path: &str) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["cat-file", "-e", &format!("{rev}:{path}")])
        .output()
        .is_ok_and(|output| output.status.success())
}

/// `Child` does not kill or reap on drop by itself — own the detached
/// `rk land` CLI helpers through both the successful path and a panic.
struct TestChild(std::process::Child);

impl Drop for TestChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// RAII teardown: stop the (real, detached) daemon this test leaves running
/// on both success and panic, so a failed assertion cannot leak it past the
/// test. Declared after the `TempDir`s it references so it drops first
/// (reverse declaration order) while the home directory (and its pid file)
/// still exist.
struct DaemonGuard {
    home: std::path::PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = rk(&self.home).args(["daemon", "stop"]).output();
    }
}

#[test]
fn graceful_stop_physically_exits_behind_a_live_reviewer_process_and_resumes_review_completion() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.toml"),
        "[disk]\nmin_free_gb = 0\n\n[harness]\ndefault = \"fake\"\n",
    )
    .unwrap();
    let workflows = home.path().join("workflows");
    std::fs::create_dir_all(&workflows).unwrap();
    std::fs::write(workflows.join("candidate-review.cue"), REVIEW_WORKFLOW).unwrap();
    let home_path = home.path().to_path_buf();
    let _guard = DaemonGuard {
        home: home_path.clone(),
    };

    let repo = candidate_repo();
    let repo_name = repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let rk_bin = env!("CARGO_BIN_EXE_rk");
    let fake = reviewer_fake_script(rk_bin);

    // The FIRST `rk` call auto-starts daemon A; `connect_or_spawn`'s detached
    // spawn inherits this process's env, so `RK_FAKE_HARNESS_CMD` set here
    // reaches every agent daemon A ever spawns, not just this one call.
    json_stdout(
        &rk(&home_path)
            .env("RK_FAKE_HARNESS_CMD", &fake)
            .args(["--json", "repo", "add", repo.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );
    let task_a = json_stdout(
        &rk(&home_path)
            .args([
                "--json",
                "ticket",
                "new",
                "substantial candidate needing review",
                "--repo",
                &repo_name,
            ])
            .output()
            .unwrap(),
    )["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let task_b = json_stdout(
        &rk(&home_path)
            .args([
                "--json",
                "ticket",
                "new",
                "doc-only queued second candidate",
                "--repo",
                &repo_name,
            ])
            .output()
            .unwrap(),
    )["identity"]
        .as_str()
        .unwrap()
        .to_string();

    // `rk land` (== `submit_manual`) does not return until the WHOLE landing
    // decision settles, including the review wait — a deliberate hang here.
    // Run it detached; the durable `landing_queue_entry` it writes BEFORE
    // blocking on the per-key drain lane is what this test actually reads.
    let lander_a = TestChild(
        rk(&home_path)
            .args([
                "land",
                "feature-a",
                "--repo",
                repo.path().to_str().unwrap(),
                "--target",
                "main",
                "--task",
                &task_a,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );

    until("candidate-a to reach awaiting_review", || {
        tuples(&home_path, &repo_name, "event", "landing_queue_entry")
            .into_iter()
            .find(|t| {
                t["payload"]["branch"] == "feature-a" && t["payload"]["status"] == "awaiting_review"
            })
    });

    let reviewer = until("the reviewer agent to spawn", || {
        let out = rk(&home_path).args(["--json", "list"]).output().unwrap();
        json_stdout(&out)
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .find(|a| a["role"].as_str() == Some("reviewer"))
    });
    let reviewer_pid = reviewer["pid"].as_u64().map(|p| p as u32);
    // Captured now, while the review context is definitely still live, as
    // the fallback verdict payload if `kill_on_drop` reaps this reviewer's
    // real process before it can reconnect and report in on its own (see
    // the finding recorded below) — read off the daemon's own binding
    // rather than recomputed, so it is never wrong.
    let review_ctx = reviewer["review"].clone();

    let pid1 = daemon_pid(&home_path).expect("daemon A must report a real pid");
    assert_ne!(
        pid1,
        std::process::id(),
        "refusing to treat this test process's own pid as the daemon's"
    );

    // Submitted only now, while candidate-a's reviewer is holding — the
    // durable `enqueue_disposition` write (inside `submit_manual`, BEFORE it
    // blocks on the per-(repo,target) drain lane) happens synchronously, so
    // this genuinely lands as `queued` before this call ever returns.
    let lander_b = TestChild(
        rk(&home_path)
            .args([
                "land",
                "feature-b",
                "--repo",
                repo.path().to_str().unwrap(),
                "--target",
                "main",
                "--task",
                &task_b,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    until("candidate-b to be durably queued", || {
        tuples(&home_path, &repo_name, "event", "landing_queue_entry")
            .into_iter()
            .find(|t| t["payload"]["branch"] == "feature-b")
    });

    // The graceful stop itself: an ordinary `rk daemon stop`, not a kill —
    // the exact CLI surface a real operator/rollover uses.
    let stop_started = Instant::now();
    let stop_out = rk(&home_path).args(["daemon", "stop"]).output().unwrap();
    assert!(
        stop_out.status.success(),
        "rk daemon stop failed: {}",
        String::from_utf8_lossy(&stop_out.stderr)
    );

    // THE bound this ticket is about: before this fix,
    // `background_tasks.join_next()` inside `Server::run` would block behind
    // `await_primary_verdict`'s poll loop for up to
    // `GateConfig::review_max_wait` (production default 45 minutes) — the
    // reviewer above is still a genuinely live OS process, blocked on a
    // marker file this test has not created. 15s is generous against the
    // ~2.6s observed for the equivalent in-process fixture; it is nowhere
    // near review_max_wait.
    let mut exited = false;
    while stop_started.elapsed() < Duration::from_secs(15) {
        if !process_alive(pid1) {
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        exited,
        "daemon A (pid {pid1}) must physically exit within a bounded time despite the \
         live marker-held reviewer process — this is the TKT-karut-jaraf-hivur regression"
    );
    let stop_elapsed = stop_started.elapsed();
    assert!(
        stop_elapsed < Duration::from_secs(15),
        "graceful stop took {stop_elapsed:?}, not bounded well clear of the margin above"
    );

    // Owned-child survival is NOT asserted here, and that gap is itself a
    // finding this fix's own BBS contract records (finding
    // 01M2HTJ67JK3XCMX89KFQWG5A7 / the follow-up published alongside it):
    // this fix stops the daemon from WAITING on the reviewer, but does
    // nothing about the harness spawn path's PRE-EXISTING `kill_on_drop(true)`
    // (`crates/rk-harness/src/lib.rs`). Once `Server::run` can genuinely
    // return promptly (the whole point of this fix), the surrounding
    // process's async runtime teardown drops every remaining task,
    // including whichever one owns the reviewer's `Child` handle — and
    // `kill_on_drop` then SIGKILLs it. Observed empirically over repeated
    // runs of THIS fixture: `rk daemon stop` returns as soon as the RPC is
    // acknowledged, well before the daemon process's own teardown reaches
    // that task, so by the time this test gets around to checking, the
    // reviewer is consistently already gone. That is an "owned harness tree"
    // question this ticket's confirmed incident never exercised (zero
    // executing/waiting checks, only a live reviewer) and a real gap between
    // "the daemon exits promptly" (this fix) and "owned child work is
    // deliberately stopped/joined, not incidentally reaped" (not this fix) —
    // recorded as remaining work rather than silently treated as covered.
    // The assertions below stay correct either way: if the reviewer DID
    // survive and reconnect, that is proven directly (a landed outcome with
    // no fallback injection below); if not, the fallback makes that explicit
    // rather than silently passing on a fabricated basis.
    eprintln!(
        "observed reviewer pid {reviewer_pid:?} alive-after-stop = {:?}",
        reviewer_pid.map(process_alive)
    );

    // The durable state a restart must resume from: no forced/fake
    // settlement was invented for candidate-a just to make shutdown fast.
    //
    // The status is checked as one of two values, not pinned to exactly
    // `awaiting_review`: this daemon runs BOTH `rk land`'s own synchronous
    // drain loop (`submit_manual`) and the periodic background `run_cycle`
    // against the same key, serialized through the same per-(repo,target)
    // lock. That lock's release, the instant this fix lets the review wait
    // bail, can let ONE more background cycle briefly re-claim and re-gate
    // the same already-prepared candidate before the outer accept loop's own
    // shutdown race (pre-existing, independent of this fix — see
    // `live_landing_restart.rs`'s "checked only BETWEEN cycles" comment)
    // finally wins. `running_gates` is exactly as resumable as
    // `awaiting_review` per this module's own restart-safety contract
    // ("a restart re-discovers a RunningGates/AwaitingReview entry... exactly
    // like a fresh Queued one") — what actually matters, and what the
    // ceiling-settlement check right below guards, is that nothing forced a
    // fake terminal decision just to exit fast.
    let a = tuples(&home_path, &repo_name, "event", "landing_queue_entry")
        .into_iter()
        .find(|t| t["payload"]["branch"] == "feature-a")
        .expect("candidate-a must survive the graceful stop untouched");
    assert!(
        matches!(
            a["payload"]["status"].as_str(),
            Some("awaiting_review" | "running_gates")
        ),
        "candidate-a must be left in a resumable in-flight status, not landed/escalated/held: {a}"
    );
    assert!(
        tuples(
            &home_path,
            &repo_name,
            "event",
            "landing_review_ceiling_settled"
        )
        .is_empty(),
        "a graceful stop must never fabricate a review-ceiling settlement just to exit fast"
    );

    // Daemon B: brought up the way the field does, an ordinary RPC through
    // `Client::connect_or_spawn` — genuinely a different OS process.
    let pid2 = start_daemon(&home_path);
    assert_ne!(
        pid1, pid2,
        "the replacement daemon must be a real different process"
    );

    // Release the marker: IF the original reviewer's real process survived
    // the stop, this would let it deliver its verdict through its ordinary
    // `rk out artifact` route, using the review context env the daemon
    // bound it with before the stop — a genuine reconnect-after-restart.
    // In practice, across every run observed during development, it does
    // NOT survive: `kill_on_drop` (module doc point 3, follow-up
    // TKT-rohib-rukaf-sizak) reaps it before this file gets anywhere near
    // this line. This write is kept because it costs nothing and remains
    // correct if that ever changes, but this test does not claim, and must
    // not be read as claiming, that the reconnect below is what actually
    // happens — see the fallback immediately below instead.
    std::fs::write(home_path.join("release-reviewer"), "").unwrap();

    let mut landed_via_reconnect = false;
    for _ in 0..200 {
        std::thread::sleep(Duration::from_millis(50));
        if ref_contains(repo.path(), "main", "src_gen.rs")
            && ref_contains(repo.path(), "main", "docs/note.md")
        {
            landed_via_reconnect = true;
            break;
        }
    }

    if !landed_via_reconnect {
        // `kill_on_drop` reaped the original reviewer (finding above) before
        // it could see the marker. Its own workflow instance settles as a
        // dead generation, which this fix's fallback path never fabricates
        // a decision for on its own — proven by `settled.is_empty()` above.
        // Reaching a landed outcome from here therefore needs an ACTUAL
        // verdict from somewhere: use the review context this test captured
        // directly off the ORIGINAL daemon's own binding (never recomputed,
        // never guessed) to stand in for a replacement reviewer's real
        // report — an explicit, documented substitution for the OS-process
        // reconnect this environment's `kill_on_drop` interaction makes
        // non-deterministic, not a route this fix's own contract relies on.
        eprintln!(
            "original reviewer did not reconnect; injecting its captured review context directly"
        );
        rk(&home_path)
            .args([
                "out", "artifact", &repo_name, "review", "--payload",
                &format!(
                    "{{\"task\":{},\"recommendation\":\"APPROVE\",\"notes\":\"resumed after graceful stop (fallback injection)\",\"branch\":{},\"head_sha\":{},\"target\":{},\"review_attempt\":{}}}",
                    review_ctx["task"], review_ctx["branch"], review_ctx["headSha"],
                    review_ctx["target"], review_ctx["attempt"],
                ),
            ])
            .output()
            .unwrap();
    }

    let mut both_landed = false;
    for _ in 0..600 {
        std::thread::sleep(Duration::from_millis(50));
        if ref_contains(repo.path(), "main", "src_gen.rs")
            && ref_contains(repo.path(), "main", "docs/note.md")
        {
            both_landed = true;
            break;
        }
    }
    if !both_landed {
        eprintln!(
            "DEBUG queue={:?}",
            tuples(&home_path, &repo_name, "event", "landing_queue_entry")
        );
        eprintln!(
            "DEBUG review_artifacts={:?}",
            tuples(&home_path, &repo_name, "artifact", "review")
        );
        eprintln!(
            "DEBUG agents={}",
            json_stdout(
                &rk(&home_path)
                    .args(["--json", "list", "--all"])
                    .output()
                    .unwrap()
            )
        );
    }
    assert!(
        both_landed,
        "both the resumed reviewed candidate and the queued second candidate must land \
         after the restart"
    );

    // At most two reviewer generations ever. The normal case observed by
    // this test is exactly one: the fallback above injects a verdict
    // directly rather than dispatching any reviewer itself, so it never
    // creates a second generation on its own. A second is still tolerated
    // (never asserted against) only because the daemon's own review-death
    // detection could independently race ahead of that injection and
    // dispatch one bounded replacement first — never more than that, which
    // is what would indicate `dispatch_review`'s idempotent re-entry for a
    // SURVIVING instance had actually spawned a duplicate instead of
    // resolving to the existing one.
    let agents = json_stdout(
        &rk(&home_path)
            .args(["--json", "list", "--all"])
            .output()
            .unwrap(),
    );
    let reviewer_count = agents
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|a| a["role"] == "reviewer" && a["repo_name"] == repo_name)
        .count();
    assert!(
        (1..=2).contains(&reviewer_count),
        "expected 1 (survived+reconnected) or 2 (reaped+one bounded replacement) reviewer \
         generations for candidate-a, got {reviewer_count}"
    );

    drop(lander_a);
    drop(lander_b);
}
