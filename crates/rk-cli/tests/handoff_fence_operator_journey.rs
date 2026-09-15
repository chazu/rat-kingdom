//! TKT-rufik-lafit-pisah P7.1, acceptance gap 3: the operator handoff
//! journey proved across REAL OS processes and the REAL public CLI, not
//! in-process futures.
//!
//! `crates/rk-daemon/src/landing.rs`'s
//! `handoff_fence_blocks_new_admission_without_draining_the_queue` proves the
//! logical half — fence engaged, A settles, B stays queued, B advances once
//! — but builds its `LandingPipeline` directly inside one test process and
//! calls `fence_request`/`fence_status` as Rust methods. It therefore cannot
//! stand as proof of the thing the ticket actually asks for: that an
//! operator can drive this with `rk fence-request`/`rk fence-status`, stop
//! and REPLACE the daemon process, and have the fence and the queue both
//! survive into the replacement.
//!
//! This file is that missing half, built on the same harness shape as
//! `bounded_daemon_stop_with_active_review.rs`: real `rk` subprocesses,
//! `Client::connect_or_spawn` auto-starting the daemon exactly as the field
//! does, a real `kill -0`-checked pid, and a tiny fake reviewer blocked on a
//! marker file this test controls. No paid provider trial, no whole-machine
//! quiescence, no fixed long sleeps — every wait is a bounded poll on a
//! condition.
//!
//! The journey asserted here, end to end:
//!  1. candidate A is held at a real marker-blocked reviewer; B is durably
//!     queued behind it;
//!  2. `rk fence-request` is acknowledged over RPC while A is mid-review;
//!  3. `rk fence-status` reports `draining` — A's lane is genuinely active;
//!  4. A is released and settles NORMALLY through its own live RPCs (the
//!     fence never cancelled or forced it);
//!  5. `rk fence-status` reports `ready` WITH B still durably queued — the
//!     whole point: readiness never requires pending entries to disappear;
//!  6. a real `rk daemon stop` physically exits that pid, and B is still
//!     queued afterwards — never silently settled to make the stop look
//!     clean;
//!  7. a REPLACEMENT daemon (a genuinely different pid) comes up and the
//!     fence is STILL engaged, read back over the public CLI — the durable
//!     store survived the rollover;
//!  8. a stale-generation `rk fence-release` is refused;
//!  9. the real `rk fence-release` lets B advance EXACTLY once.
//!
//! What this file deliberately does NOT claim: it does not prove the
//! original reviewer's OS process survives the stop and reconnects. That is
//! the pre-existing `kill_on_drop(true)` interaction recorded by
//! `bounded_daemon_stop_with_active_review.rs`'s module doc and tracked as
//! TKT-rohib-rukaf-sizak; this fixture sidesteps it entirely by settling
//! candidate A BEFORE the stop, which is exactly the operator journey the
//! ticket describes ("let that already-owned work finish via live RPCs,
//! observe ready, install/roll over").

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


/// Read `rk fence-status` over the real CLI, as JSON.
fn fence_status(home: &Path, repo: &Path) -> Value {
    json_stdout(
        &rk(home)
            .args([
                "--json",
                "fence-status",
                "--repo",
                repo.to_str().unwrap(),
            ])
            .output()
            .unwrap(),
    )
}

fn queue_entries(home: &Path, repo_name: &str) -> Vec<Value> {
    tuples(home, repo_name, "event", "landing_queue_entry")
}

fn processed(home: &Path, repo_name: &str) -> Vec<Value> {
    tuples(home, repo_name, "event", "landing_processed")
}

#[test]
fn an_operator_fences_lands_the_active_candidate_replaces_the_daemon_and_resumes_the_queue_once() {
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
    let repo_path = repo.path().to_path_buf();
    let repo_name = repo_path.file_name().unwrap().to_string_lossy().to_string();

    let rk_bin = env!("CARGO_BIN_EXE_rk");
    let fake = reviewer_fake_script(rk_bin);

    json_stdout(
        &rk(&home_path)
            .env("RK_FAKE_HARNESS_CMD", &fake)
            .args(["--json", "repo", "add", repo_path.to_str().unwrap()])
            .output()
            .unwrap(),
    );

    let task = |title: &str| {
        json_stdout(
            &rk(&home_path)
                .args(["--json", "ticket", "new", title, "--repo", &repo_name])
                .output()
                .unwrap(),
        )["identity"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let task_a = task("candidate A, held at a real reviewer barrier");
    let task_b = task("candidate B, durably queued behind the fence");

    let land = |branch: &str, task: &str| {
        TestChild(
            rk(&home_path)
                .args([
                    "land",
                    branch,
                    "--repo",
                    repo_path.to_str().unwrap(),
                    "--target",
                    "main",
                    "--task",
                    task,
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        )
    };

    // (1) A reaches a genuinely live, marker-blocked reviewer.
    let lander_a = land("feature-a", &task_a);
    until("candidate-a to reach awaiting_review", || {
        queue_entries(&home_path, &repo_name).into_iter().find(|t| {
            t["payload"]["branch"] == "feature-a" && t["payload"]["status"] == "awaiting_review"
        })
    });
    until("the reviewer agent to spawn", || {
        json_stdout(&rk(&home_path).args(["--json", "list"]).output().unwrap())
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .find(|a| a["role"].as_str() == Some("reviewer"))
    });

    let pid1 = daemon_pid(&home_path).expect("daemon A must report a real pid");
    assert_ne!(pid1, std::process::id());

    // B is durably queued behind A.
    let lander_b = land("feature-b", &task_b);
    until("candidate-b to be durably queued", || {
        queue_entries(&home_path, &repo_name)
            .into_iter()
            .find(|t| t["payload"]["branch"] == "feature-b")
    });

    // (2) The fence is requested over the REAL public CLI, while A is
    // genuinely mid-review.
    let requested = json_stdout(
        &rk(&home_path)
            .args([
                "--json",
                "fence-request",
                "--repo",
                repo_path.to_str().unwrap(),
                "--holder",
                "rollover-operator",
                "--ttl-secs",
                "900",
            ])
            .output()
            .unwrap(),
    );
    let generation = requested["generation"]
        .as_u64()
        .expect("a fence request must report its generation");
    assert_eq!(requested["holder"], "rollover-operator");
    assert_eq!(
        requested["fenced"], true,
        "the fence must be engaged on acknowledgment: {requested}"
    );

    // (3) `draining`, not `ready`: A's drain lane is genuinely held.
    let draining = fence_status(&home_path, &repo_path);
    assert_eq!(
        draining["state"], "draining",
        "A is still active, so this must not claim readiness: {draining}"
    );
    assert_eq!(draining["ready"], false, "{draining}");

    // (4) A settles NORMALLY — the fence never cancelled or forced it.
    std::fs::write(home_path.join("release-reviewer"), "").unwrap();
    until("candidate-a to land normally through its own live RPCs", || {
        ref_contains(&repo_path, "main", "src_gen.rs").then_some(())
    });

    // (5) THE readiness claim: ready WITH B still durably queued. This is
    // the whole point of a handoff window — readiness never requires the
    // queue to drain.
    let ready = until("the fence to report ready once A's lane is free", || {
        let status = fence_status(&home_path, &repo_path);
        (status["state"] == "ready").then_some(status)
    });
    assert_eq!(ready["ready"], true, "{ready}");
    assert_eq!(
        ready["blocking_keys"].as_array().unwrap().len(),
        0,
        "{ready}"
    );
    assert_eq!(
        ready["managed_blockers"].as_array().unwrap().len(),
        0,
        "no managed verify/release work is running in this fixture: {ready}"
    );
    assert!(
        queue_entries(&home_path, &repo_name)
            .iter()
            .any(|t| t["payload"]["branch"] == "feature-b"),
        "B must STILL be durably queued at the moment readiness is claimed"
    );
    assert!(
        !processed(&home_path, &repo_name)
            .iter()
            .any(|t| t["payload"]["branch"] == "feature-b"),
        "B must not have been processed while the fence is engaged"
    );

    // (5b) THE RACE THE SNAPSHOT ALONE COULD NOT CLOSE. `ready` was just
    // answered. An independent managed check now ARRIVES — after acceptance,
    // after readiness. If it were merely observed rather than fenced, it
    // would start silently and the operator would roll over on top of live
    // owned work. It must be REFUSED instead, and readiness must still hold
    // afterwards.
    let intruder = rk(&home_path)
        .args([
            "--json",
            "verify",
            "--repo",
            &repo_name,
            "--check",
            "verify",
        ])
        .output()
        .unwrap();
    assert!(
        !intruder.status.success(),
        "a managed verify.run arriving after `ready` must be refused, not admitted: {}",
        String::from_utf8_lossy(&intruder.stdout)
    );
    let refusal = String::from_utf8_lossy(&intruder.stderr).to_lowercase();
    assert!(
        refusal.contains("handoff fence"),
        "the refusal must name the handoff fence so the caller can retry after release: \
         {refusal}"
    );
    let still_ready = fence_status(&home_path, &repo_path);
    assert_eq!(
        still_ready["ready"], true,
        "a refused intruder must not have invalidated the readiness already claimed: \
         {still_ready}"
    );
    assert_eq!(
        still_ready["managed_blockers"].as_array().unwrap().len(),
        0,
        "the refused run must never have registered as owned work: {still_ready}"
    );

    // (6) The actual rollover: a real `rk daemon stop`, and a real pid exit.
    let stop_started = Instant::now();
    let stop_out = rk(&home_path).args(["daemon", "stop"]).output().unwrap();
    assert!(
        stop_out.status.success(),
        "rk daemon stop failed: {}",
        String::from_utf8_lossy(&stop_out.stderr)
    );
    let mut exited = false;
    while stop_started.elapsed() < Duration::from_secs(30) {
        if !process_alive(pid1) {
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        exited,
        "daemon A (pid {pid1}) must physically exit for the rollover to be real"
    );
    assert!(
        !ref_contains(&repo_path, "main", "docs/note.md"),
        "B must not have been silently settled to make the stop look clean"
    );

    // (7) A REPLACEMENT daemon — genuinely a different OS process — and the
    // fence is still engaged, read back over the public CLI. This is the
    // durability the whole procedure rests on: if the store had silently
    // come up empty here, the replacement daemon would resume admitting
    // work the operator believes is still fenced.
    let pid2 = start_daemon(&home_path);
    assert_ne!(
        pid1, pid2,
        "the replacement daemon must be a real different process"
    );
    let after_restart = fence_status(&home_path, &repo_path);
    assert_eq!(
        after_restart["fenced"], true,
        "the fence must survive the daemon replacement: {after_restart}"
    );
    assert_eq!(after_restart["holder"], "rollover-operator");
    assert_eq!(
        after_restart["generation"].as_u64(),
        Some(generation),
        "the surviving fence must keep its generation, not silently re-mint one"
    );
    assert!(
        queue_entries(&home_path, &repo_name)
            .iter()
            .any(|t| t["payload"]["branch"] == "feature-b"),
        "B must still be queued and resumable under the replacement daemon"
    );

    // (8) A stale/foreign holder cannot lift someone else's live fence.
    let stale = rk(&home_path)
        .args([
            "--json",
            "fence-release",
            "--repo",
            repo_path.to_str().unwrap(),
            "--holder",
            "someone-else",
            "--generation",
            &generation.to_string(),
        ])
        .output()
        .unwrap();
    assert!(
        !stale.status.success(),
        "a foreign holder must not release the fence: {}",
        String::from_utf8_lossy(&stale.stdout)
    );
    assert_eq!(
        fence_status(&home_path, &repo_path)["fenced"],
        true,
        "the refused release must leave the fence engaged"
    );

    // (9) The real release — and B advances EXACTLY once.
    json_stdout(
        &rk(&home_path)
            .args([
                "--json",
                "fence-release",
                "--repo",
                repo_path.to_str().unwrap(),
                "--holder",
                "rollover-operator",
                "--generation",
                &generation.to_string(),
            ])
            .output()
            .unwrap(),
    );
    let landed_b = |()| {
        processed(&home_path, &repo_name)
            .into_iter()
            .filter(|t| {
                t["payload"]["branch"] == "feature-b" && t["payload"]["outcome"] == "landed"
            })
            .count()
    };
    // Waited on the durable OUTCOME tuple, not just the git ref: the ref
    // moves a moment before `landing_processed` is written, so counting on
    // the ref alone races the daemon's own record.
    until("candidate-b's landed outcome to be recorded", || {
        (landed_b(()) >= 1).then_some(())
    });
    assert!(
        ref_contains(&repo_path, "main", "docs/note.md"),
        "B's landed outcome must correspond to a real merge"
    );

    // EXACTLY once, not merely at-least-once: the fence/stop/replace/resume
    // path had several chances to double-claim B (the pre-stop drain, the
    // replacement daemon's first run_cycle, and the post-release claim).
    // Hold still for a bounded window and prove the count never grows.
    let settle = Instant::now();
    while settle.elapsed() < Duration::from_secs(3) {
        assert_eq!(
            landed_b(()),
            1,
            "B must advance exactly once across the whole journey, never duplicated by the \
             fence/restart/resume path"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        queue_entries(&home_path, &repo_name).is_empty(),
        "the landing queue must be empty once both candidates have settled"
    );

    drop(lander_a);
    drop(lander_b);
}
