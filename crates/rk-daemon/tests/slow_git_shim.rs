//! Regression harness for TKT-01M04X5T98M38ECH5WJ86PK6EB item (2): proves an
//! unrelated RPC service (`agent.list`, pure in-memory registry read, no git
//! involved) stays responsive while N agent completions each drive a slow
//! `diff_summary` (crates/rk-daemon/src/supervisor.rs).
//!
//! `diff_summary_for` runs `diff_summary`'s git subprocesses via
//! `tokio::task::block_in_place` specifically so a slow `git` cannot pin the
//! async worker handling `handle_event`'s loop and, through it, starve
//! everything else sharing the runtime — the exact daemon-wedge pathology
//! fixed in d69c5ac (TKT-01M04D394PQ8VS5N3V441D1MDD). This test installs a
//! `git` shim ahead of the real one on PATH that sleeps before delegating,
//! but ONLY for the precise subprocess shapes `diff_summary` issues
//! (`Repo::rev_parse`'s bare `rev-parse <ref>`, `Repo::diff_stat`'s
//! `diff --name-only`/`diff --numstat`) — every other git call (`init`,
//! `add`, `commit`, worktree setup, `rev-parse --verify ...`) passes through
//! at full speed, so only the completion-path reads this ticket's parent
//! commit bounded are the ones under test.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

// Guards the process-global PATH/RK_FAKE_HARNESS_CMD mutation below across
// this file's tests, mirroring the HARNESS_ENV_LOCK pattern used elsewhere
// (e.g. review_tiering.rs) for the same reason: env::set_var is process-wide,
// and cargo runs a file's #[tokio::test]s concurrently by default.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Real `git` binary, resolved before PATH is shimmed so the shim has
/// something to delegate to.
fn real_git_path() -> PathBuf {
    let out = Command::new("sh")
        .arg("-c")
        .arg("command -v git")
        .output()
        .unwrap();
    assert!(out.status.success(), "no git on PATH to shim");
    PathBuf::from(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

// Placeholder-substituted rather than built with `format!`, because the
// script body is dense with literal bash `${...}` array syntax that would
// otherwise have to be escaped as `{{`/`}}` throughout.
const SHIM_TEMPLATE: &str = r#"#!/bin/bash
rest=("$@")
if [ "${rest[0]}" = "-C" ]; then
  rest=("${rest[@]:2}")
fi
slow=0
if [ "${#rest[@]}" -eq 2 ] && [ "${rest[0]}" = "rev-parse" ]; then
  slow=1
elif [ "${#rest[@]}" -eq 3 ] && [ "${rest[0]}" = "diff" ] && { [ "${rest[1]}" = "--name-only" ] || [ "${rest[1]}" = "--numstat" ]; }; then
  slow=1
fi
if [ "$slow" -eq 1 ]; then
  echo 1 >> "__COUNTER__"
  sleep __DELAY__
fi
exec "__REAL_GIT__" "$@"
"#;

/// Installs a `git` shim in `bin_dir` that sleeps `delay_secs` before
/// delegating to `real_git`, but only for `diff_summary`'s exact subprocess
/// shapes (see module docs). Every slowed call appends a line to `counter`,
/// so the caller can later prove the shim actually fired rather than the
/// test having silently exercised nothing.
fn write_slow_git_shim(bin_dir: &Path, real_git: &Path, delay_secs: u64, counter: &Path) {
    let script = SHIM_TEMPLATE
        .replace("__COUNTER__", &counter.display().to_string())
        .replace("__DELAY__", &delay_secs.to_string())
        .replace("__REAL_GIT__", &real_git.display().to_string());
    let path = bin_dir.join("git");
    std::fs::write(&path, script).unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).unwrap();
}

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "gnawed by $RK_AGENT for task $RK_TASK" > gnawed.txt
git add gnawed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"slow-git-e2e"}'
rk_done "work done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"committed gnawed.txt","session_id":"slow-git-e2e","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// N=3 rats complete concurrently, each driving a `diff_summary` whose three
/// shimmed git subprocess calls sleep 1s apiece (3s of blocked-thread time
/// per completion, sequential within `diff_summary` itself). Throughout that
/// window, a plain `agent.list` call — no git, no filesystem, a registry read
/// under a std::sync::Mutex held only for the read itself — must keep
/// answering in well under a second. Before `block_in_place` wrapped these
/// calls, running them directly on the tokio worker executing `handle_event`
/// would have pinned that worker (and, once enough completions land at once,
/// every worker) for the full 3s+ each, exactly the daemon-wedge pathology
/// TKT-01M04D394PQ8VS5N3V441D1MDD reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_rpc_stays_responsive_during_slow_git_completions() {
    let _env_guard = ENV_LOCK.lock().await;
    let original_path = std::env::var("PATH").unwrap_or_default();

    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    let real_git = real_git_path();
    let shim_dir = tempfile::tempdir().unwrap();
    let counter = shim_dir.path().join("slow-calls.log");
    write_slow_git_shim(shim_dir.path(), &real_git, 1, &counter);

    std::env::set_var(
        "PATH",
        format!("{}:{}", shim_dir.path().display(), original_path),
    );
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "slow-git-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    const N: usize = 3;
    let mut names = Vec::new();
    for i in 0..N {
        let spawned = client
            .call(
                "agent.spawn",
                json!({
                    "repo": repo_dir.path().to_string_lossy(),
                    "task": format!("gnaw-{i}"),
                    "harness": "fake",
                }),
            )
            .await
            .unwrap();
        names.push(spawned["agent"]["name"].as_str().unwrap().to_string());
    }

    // Poll an unrelated RPC while the N completions (and their slow
    // diff_summary calls) are in flight, tracking both the worst-case
    // latency of that RPC and whether every agent has settled.
    let mut max_latency = Duration::ZERO;
    let mut samples = 0usize;
    let mut all_completed = false;
    for _ in 0..200 {
        let started = tokio::time::Instant::now();
        let listed = client.call("agent.list", json!({})).await.unwrap();
        max_latency = max_latency.max(started.elapsed());
        samples += 1;

        let states: Vec<String> = listed["agents"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| names.contains(&a["name"].as_str().unwrap_or_default().to_string()))
            .map(|a| a["state"].as_str().unwrap_or_default().to_string())
            .collect();
        if states.len() == N && states.iter().all(|s| s == "completed") {
            all_completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        all_completed,
        "not all {N} agents reached `completed` within the polling window"
    );
    assert!(
        samples >= N,
        "too few latency samples taken to be meaningful: {samples}"
    );
    assert!(
        max_latency < Duration::from_millis(500),
        "an unrelated RPC (agent.list) was blocked for {max_latency:?} while {N} slow git \
         completions were in flight — diff_summary_for must keep the git subprocess off the \
         async worker (block_in_place) so unrelated RPCs stay responsive"
    );

    // Confirm the shim was actually exercised — 3 slowed calls per
    // completion (one rev-parse, two diff) — so a vacuously fast run (e.g.
    // the shim silently failing to intercept) cannot pass as "responsive".
    let slow_calls = std::fs::read_to_string(&counter).unwrap_or_default();
    let slow_call_count = slow_calls.lines().filter(|l| !l.is_empty()).count();
    assert!(
        slow_call_count >= N * 3,
        "shim only recorded {slow_call_count} slow git calls, expected at least {} \
         (rev-parse + 2x diff per completion) — the shim did not intercept diff_summary's \
         git subprocess calls",
        N * 3
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
    std::env::set_var("PATH", original_path);
}
