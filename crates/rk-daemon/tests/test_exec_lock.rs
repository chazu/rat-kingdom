//! TKT-01M0CFA1RX36SJ7DV4YWGHQ9BT: the shared-CARGO_TARGET_DIR test-execution
//! lock. Cargo's own target-dir lock only covers a single invocation's
//! *build* phase; it is released as soon as that invocation's build
//! finishes, before it execs the test binaries it just resolved paths for.
//! Under `[disk] shared_cargo_target`, a second concurrent invocation against
//! the same shared dir can recompile and garbage-collect a binary the first
//! is about to exec in that gap, producing `could not execute process ...
//! (never executed) ... No such file or directory`
//! (docs/2026-08-19-tkt-hot-scan-target-dir-contention.md).
//!
//! `TestExecLock` closes the gap by serializing a repo-registered check's
//! *entire* run (not just the exec sliver — that boundary is not observable
//! from outside cargo) against every other same-repo check that opts in via
//! `sharedCargoTarget: true`. These tests prove three things:
//!
//! 1. Two same-repo checks that both opt in, with `shared_cargo_target` on,
//!    never run concurrently — real mutual exclusion, not a timing artifact.
//! 2. The same two checks, with `shared_cargo_target` off, genuinely DO run
//!    concurrently — proving (1) is the lock at work, not some incidental
//!    serialization already present in the daemon.
//! 3. A check that does NOT opt in is never serialized, even with
//!    `shared_cargo_target` on — the lock is scoped per check, so an
//!    unrelated fast gate (a git diff-scope check) never queues behind a
//!    slow `verify` run for the same repo.
//!
//! Mutual exclusion (1) is a hard guarantee of `tokio::sync::Mutex` and is
//! never flaky. Genuine concurrency (2)/(3) is proven the same way
//! `merge_queue.rs` proves its own race: two invocations kicked off together
//! on a multi-thread runtime with a short critical section overlap on any
//! real machine, the same accepted style already used there.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
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

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work for $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"tel-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"tel-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Records how many concurrent invocations were ever alive at once (`peak`)
/// into `$RK_CHECK_MARKER_DIR`, via a non-atomic read-increment-write —
/// exactly the shape of race that reveals a missing mutual exclusion.
const CONTENDED_COMMAND: &str = r#"n=$(( $(cat "$RK_CHECK_MARKER_DIR/count" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$RK_CHECK_MARKER_DIR/count"; peak=$(cat "$RK_CHECK_MARKER_DIR/peak" 2>/dev/null || echo 0); if [ "$n" -gt "$peak" ]; then echo "$n" > "$RK_CHECK_MARKER_DIR/peak"; fi; sleep 0.4; n2=$(( $(cat "$RK_CHECK_MARKER_DIR/count") - 1 )); echo "$n2" > "$RK_CHECK_MARKER_DIR/count""#;

fn checks_cue(shared_cargo_target: bool) -> String {
    format!(
        r#"
checks: [
    {{
        name: "contended",
        command: {command:?},
        timeout: "10s",
        sharedCargoTarget: {flag},
    }},
]
"#,
        command = CONTENDED_COMMAND,
        flag = shared_cargo_target,
    )
}

const CONTENDED_WORKFLOW: &str = r#"
workflow: {
    name: "contended"
    params: {taskId: {type: "string", required: true}, markerDir: {type: "string", required: true}}
    agents: {default: {harness: "fake", model: "sonnet"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {
            type: "run"
            check: "contended"
            env: {RK_CHECK_MARKER_DIR: _input.markerDir}
        },
        {type: "dismiss"},
    ]
}
"#;

fn init_repo(repo: &Path) {
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# x\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
}

fn write_def(repo: &Path, name: &str, src: &str) {
    let wf_dir = repo.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join(format!("{name}.cue")), src).unwrap();
}

fn write_checks(repo: &Path, src: &str) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), src).unwrap();
}

async fn await_status(client: &mut Client, id: &str, want: &str) -> serde_json::Value {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        match status["instance"]["status"].as_str().unwrap_or("") {
            s if s == want => return status,
            "failed" if want != "failed" => {
                panic!(
                    "workflow failed unexpectedly: {}",
                    status["instance"]["error"]
                )
            }
            _ => {}
        }
    }
    panic!("workflow never reached status {want}");
}

fn read_peak(marker_dir: &Path) -> u64 {
    std::fs::read_to_string(marker_dir.join("peak"))
        .unwrap_or_else(|_| "0".into())
        .trim()
        .parse()
        .unwrap()
}

/// Fire two `contended` runs concurrently against the same repo, each its own
/// rat/worktree, and return the peak concurrent occupancy the check observed.
async fn run_two_concurrently(layout: &Layout, repo: &Path, marker_dir: &Path) -> u64 {
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    let mut handles = Vec::with_capacity(2);
    for i in 0..2 {
        let layout = layout.clone();
        let repo = repo.to_path_buf();
        let marker_dir = marker_dir.to_path_buf();
        handles.push(tokio::spawn(async move {
            let mut client = connect(&layout).await;
            let started = client
                .call(
                    "workflow.run",
                    json!({
                        "name": "contended",
                        "repo": repo.to_string_lossy(),
                        "params": {
                            "taskId": format!("tel-{i}"),
                            "markerDir": marker_dir.to_string_lossy(),
                        },
                    }),
                )
                .await
                .unwrap();
            let id = started["instance"]["id"].as_str().unwrap().to_string();
            await_status(&mut client, &id, "completed").await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
    read_peak(marker_dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_cargo_target_on_serializes_opted_in_checks() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(repo_dir.path(), "contended", CONTENDED_WORKFLOW);
    write_checks(repo_dir.path(), &checks_cue(true));

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_shared_cargo_target(true);
    let _handle = tokio::spawn(daemon.run());

    let peak = run_two_concurrently(&layout, repo_dir.path(), marker_dir.path()).await;
    assert_eq!(
        peak, 1,
        "TestExecLock must serialize two same-repo checks that both opt in \
         via sharedCargoTarget when shared_cargo_target is on"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_cargo_target_off_lets_opted_in_checks_race() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(repo_dir.path(), "contended", CONTENDED_WORKFLOW);
    write_checks(repo_dir.path(), &checks_cue(true));

    let layout = Layout::at(home.path());
    // shared_cargo_target left OFF (default) — the lock must have zero
    // effect even though the check itself opts in, proving the daemon-level
    // flag is a genuine opt-out and not just decoration.
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());

    let peak = run_two_concurrently(&layout, repo_dir.path(), marker_dir.path()).await;
    assert_eq!(
        peak, 2,
        "without shared_cargo_target on, two concurrent checks must genuinely \
         overlap — this is the baseline race the lock exists to close"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checks_that_do_not_opt_in_are_never_serialized() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    write_def(repo_dir.path(), "contended", CONTENDED_WORKFLOW);
    // sharedCargoTarget left false on the check itself.
    write_checks(repo_dir.path(), &checks_cue(false));

    let layout = Layout::at(home.path());
    // shared_cargo_target ON at the daemon level — proves scoping is per
    // check, not just gated on the daemon flag alone.
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_shared_cargo_target(true);
    let _handle = tokio::spawn(daemon.run());

    let peak = run_two_concurrently(&layout, repo_dir.path(), marker_dir.path()).await;
    assert_eq!(
        peak, 2,
        "a check that never set sharedCargoTarget must never be serialized, \
         even when shared_cargo_target is on fleet-wide"
    );
}
