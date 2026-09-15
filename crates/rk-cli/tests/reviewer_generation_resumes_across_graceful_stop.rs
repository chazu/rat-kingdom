//! TKT-ravig-kumob-timuh: genuine cross-*process* proof that a reviewer
//! interrupted by a deliberate graceful `rk daemon stop` resumes as the SAME
//! generation — not a bounded-replacement dispatch — once the replacement
//! daemon comes up, and that its own resumed process is what settles the
//! pending landing, never a value this test computed or injected on its
//! behalf.
//!
//! `crates/rk-cli/tests/bounded_daemon_stop_with_active_review.rs` proves the
//! OLD daemon's real OS pid exits within a bound despite a live reviewer
//! process; this file is the other half the reviewer flagged as missing
//! (landing artifact 01M2JBVMACK74MAQC5HXYJ1SJ7): that the interrupted
//! reviewer's own `AgentRecord` — exact `spawn` id, `review` binding,
//! accumulating `cost_usd` — is what resumes, proven by never injecting a
//! verdict and instead letting a genuinely relaunched reviewer subprocess
//! publish its own `rk out artifact ... review` through the real
//! `Supervisor::respawn_generation` path (`rk respawn`, the same primitive
//! `rk daemon rollover` already uses for a rat).
//!
//! Two real daemon OS processes, like `bounded_daemon_stop_with_active_review.rs`
//! and `resumed_generation_successor_landing.rs` before it — not two
//! in-process `Daemon` instances sharing one home (see BBS finding
//! 01M2JGGEN6Y6AQMMW3N1GFNY33: a surviving background task from the outgoing
//! daemon can silently roll back the replacement's write in that shape). The
//! fake reviewer harness blocks on a marker file this test controls so its
//! liveness at stop time, and its readiness to complete once relaunched, are
//! both deterministic rather than timing-dependent.

use serde_json::Value;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// Every `rk` invocation in this file carries the fake-reviewer script, not
/// just the one that happens to trigger the first `connect_or_spawn`: ANY
/// call made after `rk daemon stop` (including a plain `scan`/`list` status
/// check) can be the one that auto-starts the replacement daemon, and
/// `spawn_detached_daemon` inherits only THAT invocation's env — a daemon
/// started by some other, unrelated call never sees it.
fn rk(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
    cmd.env("RK_FAKE_HARNESS_CMD", reviewer_fake_script());
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

/// Bring a daemon up the way the field does — an ordinary RPC call through
/// `Client::connect_or_spawn`, via the `rk` helper above (which always
/// carries `RK_FAKE_HARNESS_CMD`) — so the detached daemon spawn (which
/// inherits this call's env) uses it for every agent it launches over its
/// whole lifetime, including one launched much later by an explicit
/// `rk respawn`.
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

fn agents(home: &Path) -> Vec<Value> {
    json_stdout(&rk(home).args(["--json", "list", "--all"]).output().unwrap())
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn ref_contains(repo: &Path, rev: &str, path: &str) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["cat-file", "-e", &format!("{rev}:{path}")])
        .output()
        .is_ok_and(|output| output.status.success())
}

/// A repo with one unlanded branch genuinely non-trivial enough to route
/// through review (`classify_diff` in `supervisor.rs` only sends past-trivial
/// diffs through review — the same 80-line trick
/// `bounded_daemon_stop_with_active_review.rs`/`review_ceiling_crash_barrier.rs`
/// use).
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

/// Blocks on a marker file this test controls, then delivers its verdict
/// through the SAME `rk out artifact` route a real reviewer's LLM turn
/// would, using its own `RK_REVIEW_*` env bindings — never a value this test
/// computes or injects on its behalf. Runs identically whether this is the
/// original launch or a later `rk respawn` relaunch of the same generation.
fn reviewer_fake_script() -> String {
    let rk_bin = env!("CARGO_BIN_EXE_rk");
    format!(
        r#"
read -r _prompt
i=0
while [ ! -f "$RK_HOME/release-reviewer" ] && [ "$i" -lt 600 ]; do
  sleep 0.05
  i=$((i+1))
done
"{rk_bin}" out artifact "$RK_REPO" review --payload "{{\"task\":\"$RK_REVIEW_TASK\",\"recommendation\":\"APPROVE\",\"notes\":\"genuine resumed-generation reviewer report\",\"branch\":\"$RK_REVIEW_BRANCH\",\"head_sha\":\"$RK_REVIEW_HEAD\",\"target\":\"$RK_REVIEW_TARGET\",\"review_attempt\":\"$RK_REVIEW_ATTEMPT\"}}" >/dev/null 2>&1
echo '{{"type":"system","subtype":"init","session_id":"marker-reviewer"}}'
"{rk_bin}" done "review complete" >/dev/null 2>&1
echo '{{"type":"result","subtype":"success","is_error":false,"result":"reviewed","session_id":"marker-reviewer","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#
    )
}

/// `Child` does not kill or reap on drop by itself.
struct TestChild(std::process::Child);

impl Drop for TestChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// RAII teardown: stop the (real, detached) daemon this test leaves running,
/// so a failed assertion cannot leak it past the test.
struct DaemonGuard {
    home: std::path::PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = rk(&self.home).args(["daemon", "stop"]).output();
    }
}

#[test]
fn reviewer_generation_resumes_across_graceful_stop_with_no_injected_verdict() {
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

    // Daemon A: the first `rk` call auto-starts it, and its detached spawn
    // inherits THIS call's env (every `rk()` invocation carries
    // `RK_FAKE_HARNESS_CMD`), so it reaches every agent daemon A ever
    // launches.
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

    // `rk land` blocks until the whole landing decision settles; run it
    // detached and read the durable `landing_queue_entry` it writes before
    // blocking on the review wait.
    let lander = TestChild(
        rk(&home_path)
            .args([
                "land",
                "feature-a",
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
        agents(&home_path)
            .into_iter()
            .find(|a| a["role"].as_str() == Some("reviewer"))
    });
    let reviewer_name = reviewer["name"].as_str().unwrap().to_string();
    let original_spawn = reviewer["spawn"].as_str().unwrap().to_string();
    let original_review = reviewer["review"].clone();
    let original_cost = reviewer["cost_usd"].as_f64().unwrap_or(0.0);
    // `spawning` briefly precedes `running` (the `Started` handshake); wait
    // for genuine liveness rather than a single racy snapshot.
    until("the reviewer to reach Running before the stop", || {
        agents(&home_path)
            .into_iter()
            .find(|a| a["name"].as_str() == Some(reviewer_name.as_str()))
            .filter(|a| a["state"].as_str() == Some("running"))
    });

    let pid1 = daemon_pid(&home_path).expect("daemon A must report a real pid");
    assert_ne!(
        pid1,
        std::process::id(),
        "refusing to treat this test process's own pid as the daemon's"
    );

    // The graceful stop: an ordinary `rk daemon stop`, the exact CLI surface
    // a real operator/rollover uses.
    let stop_started = Instant::now();
    let stop_out = rk(&home_path).args(["daemon", "stop"]).output().unwrap();
    assert!(
        stop_out.status.success(),
        "rk daemon stop failed: {}",
        String::from_utf8_lossy(&stop_out.stderr)
    );

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
         live marker-held reviewer process"
    );

    // The durable landing state a restart must resume from: no forced/fake
    // settlement was invented for candidate-a just to make shutdown fast.
    let a = tuples(&home_path, &repo_name, "event", "landing_queue_entry")
        .into_iter()
        .find(|t| t["payload"]["branch"] == "feature-a")
        .expect("candidate-a must survive the graceful stop untouched");
    assert!(
        matches!(
            a["payload"]["status"].as_str(),
            Some("awaiting_review" | "running_gates")
        ),
        "candidate-a must be left in a resumable in-flight status: {a}"
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

    // THE core acceptance proof: the reviewer's record must be left
    // `Orphaned` — genuinely resumable, the same disposition a rat gets —
    // never routed through the crash arm to `Failed`, and never replaced by
    // a bounded-replacement dispatch (still exactly one reviewer record for
    // this repo).
    let orphaned = agents(&home_path)
        .into_iter()
        .find(|a| a["name"].as_str() == Some(reviewer_name.as_str()))
        .expect("the reviewer's record must survive the stop");
    assert_eq!(
        orphaned["state"].as_str(),
        Some("orphaned"),
        "a deliberately-stopped reviewer must be left Orphaned (resumable), not routed through \
         the crash arm to Failed: {orphaned}"
    );
    assert_eq!(
        orphaned["spawn"].as_str(),
        Some(original_spawn.as_str()),
        "the generation identity must be unchanged by the stop"
    );
    let reviewer_records_after_stop = agents(&home_path)
        .into_iter()
        .filter(|a| a["role"] == "reviewer" && a["repo_name"] == repo_name)
        .count();
    assert_eq!(
        reviewer_records_after_stop, 1,
        "a graceful stop must never itself dispatch a bounded-replacement reviewer generation"
    );

    // Daemon B: a genuinely different OS process, brought up the way the
    // field does, with the same fake-harness wiring so a later relaunch of
    // the SAME reviewer generation runs the real script again.
    let pid2 = start_daemon(&home_path);
    assert_ne!(
        pid1, pid2,
        "the replacement daemon must be a real different process"
    );

    // Release the marker before resuming, so the relaunched reviewer
    // completes as soon as it runs rather than blocking again.
    std::fs::write(home_path.join("release-reviewer"), "").unwrap();

    // The native resume itself: the same primitive `rk daemon rollover`
    // already uses for a rat (`Supervisor::respawn_generation`), now reached
    // for a reviewer because its record reads `Orphaned` instead of `Failed`.
    // No injected verdict anywhere in this test — what follows is the
    // resumed process's own report.
    let respawn_out = rk(&home_path)
        .args(["--json", "respawn", &reviewer_name])
        .output()
        .unwrap();
    assert!(
        respawn_out.status.success(),
        "rk respawn {reviewer_name} failed: {}",
        String::from_utf8_lossy(&respawn_out.stderr)
    );
    let respawned = json_stdout(&respawn_out);
    let respawned_spawn = respawned["agent"]["spawn"]
        .as_str()
        .or_else(|| respawned["spawn"].as_str())
        .expect("respawn response must carry the agent's spawn id");
    assert_eq!(
        respawned_spawn, original_spawn,
        "`rk respawn` must continue the SAME generation, not mint a new one"
    );

    // Wait for the resumed reviewer's own authenticated verdict — written by
    // its own real subprocess, using the review binding carried through the
    // respawn, never recomputed by this test.
    let verdict = until("the resumed reviewer's own verdict artifact", || {
        tuples(&home_path, &repo_name, "artifact", "review")
            .into_iter()
            .find(|t| {
                t["payload"]["review_attempt"].as_str() == original_review["attempt"].as_str()
            })
    });
    assert_eq!(
        verdict["payload"]["recommendation"].as_str(),
        Some("APPROVE")
    );
    assert_eq!(
        verdict["payload"]["branch"].as_str(),
        original_review["branch"].as_str(),
        "the resumed reviewer's binding must match the original, not a recomputed one"
    );
    assert_eq!(
        verdict["payload"]["head_sha"].as_str(),
        original_review["headSha"].as_str()
    );
    assert_eq!(
        verdict["payload"]["target"].as_str(),
        original_review["target"].as_str()
    );

    // The candidate must actually land from that real verdict.
    let mut landed = false;
    for _ in 0..600 {
        std::thread::sleep(Duration::from_millis(50));
        if ref_contains(repo.path(), "main", "src_gen.rs") {
            landed = true;
            break;
        }
    }
    if !landed {
        eprintln!(
            "DEBUG queue={:?}",
            tuples(&home_path, &repo_name, "event", "landing_queue_entry")
        );
        eprintln!("DEBUG agents={:?}", agents(&home_path));
    }
    assert!(
        landed,
        "candidate-a must land from the resumed reviewer's real verdict"
    );

    // Final proof: exactly one reviewer generation ever existed for this
    // candidate — the SAME `spawn` id from before the stop — and its cost
    // ledger only grew, never reset by the resume.
    let final_agents = agents(&home_path);
    let reviewer_records: Vec<Value> = final_agents
        .into_iter()
        .filter(|a| a["role"] == "reviewer" && a["repo_name"] == repo_name)
        .collect();
    assert_eq!(
        reviewer_records.len(),
        1,
        "exactly one reviewer generation must exist end to end — no bounded-replacement was \
         ever dispatched: {reviewer_records:?}"
    );
    let final_record = &reviewer_records[0];
    assert_eq!(
        final_record["spawn"].as_str(),
        Some(original_spawn.as_str())
    );
    assert_eq!(final_record["state"].as_str(), Some("completed"));
    assert!(
        final_record["cost_usd"].as_f64().unwrap_or(0.0) >= original_cost,
        "cost must never roll backward across a resume: before {original_cost}, after {final_record}"
    );

    drop(lander);
}
