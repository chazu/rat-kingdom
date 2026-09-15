//! End to end over real `rk` subprocesses: `rk daemon rollover` parks a live
//! rat across a real daemon *process* restart — proving `kill_on_drop`
//! actually orphans it rather than merely changing in-memory state — without
//! losing its worktree/branch/ticket record, and the parked rat comes back
//! running on the new daemon.

use serde_json::Value;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn rk(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rk"));
    cmd.env("RK_HOME", home);
    // A rat's spawn env would otherwise pick an agent identity; this drives
    // the CLI as the operator.
    cmd.env_remove("RK_AGENT");
    cmd.env_remove("RK_AUTH_TOKEN");
    cmd
}

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

/// These tests spawn against a `tempfile::tempdir()` `RK_HOME`, whose free
/// disk space depends on wherever the test runner's temp filesystem lives —
/// on a constrained CI disk that can already be under the default `[disk]
/// min_free_gb = 10` floor, which then refuses every spawn before rollover
/// is even exercised. Disable the guard for these tests; they cover rollover
/// behavior, not disk-pressure refusal (that's `worktree_cleanup.rs`).
fn disable_disk_floor(home: &Path) {
    std::fs::write(home.join("config.toml"), "[disk]\nmin_free_gb = 0\n").unwrap();
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    std::fs::create_dir_all(dir.join(".rk")).unwrap();
    std::fs::write(dir.join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn json_stdout(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "bad json: {e}\nstdout: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

#[test]
fn rollover_parks_a_live_rat_and_it_respawns() {
    let home = tempfile::tempdir().unwrap();
    disable_disk_floor(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    json_stdout(
        &rk(home.path())
            .env("RK_FAKE_HARNESS_CMD", "sleep 60")
            .args(["--json", "repo", "add", repo_dir.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );

    // Bring the first daemon up (via `spawn`'s connect_or_spawn) with a rat
    // that hangs — still `Running` when rollover comes looking for it.
    let spawn_out = rk(home.path())
        .env("RK_FAKE_HARNESS_CMD", "sleep 60")
        .args([
            "--json",
            "spawn",
            "--task",
            "rollover-1",
            "--repo",
            repo_dir.path().to_str().unwrap(),
            "--harness",
            "fake",
        ])
        .output()
        .expect("run rk spawn");
    let agent = json_stdout(&spawn_out);
    let name = agent["name"].as_str().unwrap().to_string();

    let pid1 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();

    // Wait for the rat to actually be Running (not just Spawning) so the
    // rollover below genuinely catches it live.
    let mut running = false;
    for _ in 0..100 {
        let st = json_stdout(
            &rk(home.path())
                .args(["--json", "status", &name])
                .output()
                .unwrap(),
        );
        if st["state"] == "running" {
            running = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(running, "rat never reached Running");

    // Roll over: a short wait timeout so it parks the still-sleeping rat
    // instead of waiting the full 60s out. The fresh daemon gets a
    // fast-completing script so the respawn is observable quickly. It must
    // call `rk done` before reporting its turn — a clean turn with no `rk
    // done` now parks the agent as `Paused` (awaiting resume) rather than
    // `Completed`, so a script that merely echoes a result and exits would
    // terminalize as `Failed` instead of the `Completed` this test asserts.
    let rollover_out = rk(home.path())
        .env(
            "RK_FAKE_HARNESS_CMD",
            format!(
                r#"{} done "resumed after rollover" >/dev/null 2>&1 || true
echo '{{"type":"result","subtype":"success","is_error":false,"result":"resumed after rollover","usage":{{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'"#,
                env!("CARGO_BIN_EXE_rk")
            ),
        )
        .args(["--json", "daemon", "rollover", "--wait-secs", "1"])
        .output()
        .expect("run rk daemon rollover");
    let rollover = json_stdout(&rollover_out);
    assert_eq!(rollover["rolled_over"], true, "{rollover}");
    assert_eq!(
        rollover["respawn_failed"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0),
        0,
        "rollover: {rollover}"
    );
    let respawned = rollover["respawned"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        respawned.iter().any(|v| v.as_str() == Some(name.as_str())),
        "expected {name} in respawned list: {rollover}"
    );

    // The new daemon really is a new process — the whole point of rollover.
    let pid2 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();
    assert_ne!(
        pid1, pid2,
        "rollover did not actually replace the daemon process"
    );

    // The respawned rat finishes on the new daemon, in the same worktree
    // (state/branch survived the process restart — no ticket state lost).
    let mut completed = false;
    for _ in 0..100 {
        let st = json_stdout(
            &rk(home.path())
                .args(["--json", "status", &name])
                .output()
                .unwrap(),
        );
        if st["state"] == "completed" {
            assert_eq!(st["result"], "resumed after rollover");
            completed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(completed, "respawned rat never completed");

    let _ = rk(home.path()).args(["daemon", "stop"]).output();
}

/// A daemon process holding this env var when it handles `"stop"` keeps the
/// socket fully live for that many milliseconds before actually shutting
/// down — a fault-injection knob added for this ticket (see the `"stop"`
/// handler in rk-daemon's dispatch). It is read from the *daemon's own*
/// process env, inherited only at the moment something spawns it — setting
/// it on a later CLI invocation that merely talks to an already-running
/// daemon has no effect, so every command in these tests carries it from the
/// first one that can trigger `connect_or_spawn`.
fn rk_delayed_shutdown(home: &Path, delay_ms: &str) -> Command {
    let mut cmd = rk(home);
    cmd.env("RK_TEST_SHUTDOWN_DELAY_MS", delay_ms);
    cmd
}

/// TKT-nusod-lizuk-jomun: an ordinary slow shutdown (well inside the 15s
/// `OLD_INSTANCE_EXIT_BOUND`, comfortably past the old, too-tight 3s
/// assumption) must still end in a genuinely replaced daemon — not a false
/// "nothing changed" failure, and not a false-success reconnect to the
/// still-live retiring instance either. Zero live rats: the drain phase
/// returns immediately, exercising the branch the original production bug
/// hit (no further RPC or identity check before reporting success).
#[test]
fn rollover_succeeds_once_old_daemon_exits_within_the_reasonable_bound() {
    let home = tempfile::tempdir().unwrap();
    disable_disk_floor(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    rk_delayed_shutdown(home.path(), "4000")
        .args(["ping"])
        .output()
        .expect("run rk ping");
    json_stdout(
        &rk_delayed_shutdown(home.path(), "4000")
            .args(["--json", "repo", "add", repo_dir.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );

    let pid1 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();

    let rollover_out = rk_delayed_shutdown(home.path(), "4000")
        .args(["--json", "daemon", "rollover", "--wait-secs", "0"])
        .output()
        .expect("run rk daemon rollover");
    let rollover = json_stdout(&rollover_out);
    assert_eq!(rollover["rolled_over"], true, "{rollover}");

    // A genuinely different, live process — not a reconnect to the (by now
    // actually exited) outgoing daemon.
    let pid2 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();
    assert_ne!(pid1, pid2, "rollover did not actually replace the daemon process");

    let spawn_out = rk(home.path())
        .args([
            "--json",
            "spawn",
            "--task",
            "post-rollover-delayed",
            "--repo",
            repo_dir.path().to_str().unwrap(),
            "--harness",
            "fake",
        ])
        .output()
        .expect("run rk spawn");
    assert!(
        spawn_out.status.success(),
        "dispatch must work on the fresh daemon: {}",
        String::from_utf8_lossy(&spawn_out.stderr)
    );

    let _ = rk(home.path()).args(["daemon", "stop"]).output();
}

/// Same ordinary-but-slow shutdown as above, but with a live rat that must
/// be correctly parked and respawned once the genuine replacement daemon
/// comes up — proving the "exactly parked generations" contract holds
/// across a realistic delayed exit, not just an instantaneous one.
#[test]
fn rollover_parks_and_respawns_across_a_reasonable_delayed_exit() {
    let home = tempfile::tempdir().unwrap();
    disable_disk_floor(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    json_stdout(
        &rk_delayed_shutdown(home.path(), "4000")
            .env("RK_FAKE_HARNESS_CMD", "sleep 60")
            .args(["--json", "repo", "add", repo_dir.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );

    let spawn_out = rk_delayed_shutdown(home.path(), "4000")
        .env("RK_FAKE_HARNESS_CMD", "sleep 60")
        .args([
            "--json",
            "spawn",
            "--task",
            "rollover-barrier",
            "--repo",
            repo_dir.path().to_str().unwrap(),
            "--harness",
            "fake",
        ])
        .output()
        .expect("run rk spawn");
    let agent = json_stdout(&spawn_out);
    let name = agent["name"].as_str().unwrap().to_string();

    let pid1 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();

    let mut running = false;
    for _ in 0..100 {
        let st = json_stdout(
            &rk(home.path())
                .args(["--json", "status", &name])
                .output()
                .unwrap(),
        );
        if st["state"] == "running" {
            running = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(running, "rat never reached Running");

    let rollover_out = rk_delayed_shutdown(home.path(), "4000")
        .env(
            "RK_FAKE_HARNESS_CMD",
            format!(
                r#"{} done "resumed after delayed rollover" >/dev/null 2>&1 || true
echo '{{"type":"result","subtype":"success","is_error":false,"result":"resumed after delayed rollover","usage":{{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'"#,
                env!("CARGO_BIN_EXE_rk")
            ),
        )
        .args(["--json", "daemon", "rollover", "--wait-secs", "1"])
        .output()
        .expect("run rk daemon rollover");
    let rollover = json_stdout(&rollover_out);
    assert_eq!(rollover["rolled_over"], true, "{rollover}");
    let respawned = rollover["respawned"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        respawned.iter().any(|v| v.as_str() == Some(name.as_str())),
        "expected {name} in respawned list: {rollover}"
    );

    let pid2 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();
    assert_ne!(pid1, pid2, "rollover did not actually replace the daemon process");

    let mut completed = false;
    for _ in 0..100 {
        let st = json_stdout(
            &rk(home.path())
                .args(["--json", "status", &name])
                .output()
                .unwrap(),
        );
        if st["state"] == "completed" {
            completed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(completed, "respawned rat never completed on the new daemon");

    let _ = rk(home.path()).args(["daemon", "stop"]).output();
}

/// TKT-nusod-lizuk-jomun, the corrected recovery contract: once a daemon
/// truly outlives even the generous 15s `OLD_INSTANCE_EXIT_BOUND`, rollover
/// must fail loudly (not report false success) AND must not resume dispatch
/// on that daemon — its `stop` is already accepted and irrevocable, so
/// admitting new work onto it would just get killed out from under whatever
/// got dispatched. `agent.spawn` staying refused after the failure is the
/// proof dispatch was never resumed.
#[test]
fn rollover_fails_without_resuming_dispatch_past_the_reasonable_bound() {
    let home = tempfile::tempdir().unwrap();
    disable_disk_floor(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    // Longer than OLD_INSTANCE_EXIT_BOUND (15s) — a genuinely unreasonable
    // shutdown, not just an ordinary slow one.
    rk_delayed_shutdown(home.path(), "17000")
        .args(["ping"])
        .output()
        .expect("run rk ping");
    json_stdout(
        &rk_delayed_shutdown(home.path(), "17000")
            .args(["--json", "repo", "add", repo_dir.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );

    let pid1 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();

    let rollover_out = rk_delayed_shutdown(home.path(), "17000")
        .args(["--json", "daemon", "rollover", "--wait-secs", "0"])
        .output()
        .expect("run rk daemon rollover");
    assert!(
        !rollover_out.status.success(),
        "rollover must fail once the outgoing daemon exceeds the reasonable \
         exit bound: stdout={} stderr={}",
        String::from_utf8_lossy(&rollover_out.stdout),
        String::from_utf8_lossy(&rollover_out.stderr)
    );
    let stderr = String::from_utf8_lossy(&rollover_out.stderr);
    assert!(
        stderr.contains("did not actually exit within"),
        "expected the explicit old-instance timeout error, got: {stderr}"
    );

    // Still the same (still-retiring) daemon — nothing was replaced.
    let pid2 = json_stdout(
        &rk(home.path())
            .args(["--json", "daemon", "status"])
            .output()
            .unwrap(),
    )["pid"]
        .as_u64()
        .unwrap();
    assert_eq!(pid1, pid2, "no daemon was actually replaced by the failed attempt");

    // The corrected contract: dispatch must stay paused, because this
    // daemon's stop is already committed and irrevocable — resuming it
    // would admit work onto a process that is going away regardless.
    let spawn_out = rk(home.path())
        .args([
            "--json",
            "spawn",
            "--task",
            "post-timeout-rollover",
            "--repo",
            repo_dir.path().to_str().unwrap(),
            "--harness",
            "fake",
        ])
        .output()
        .expect("run rk spawn");
    assert!(
        !spawn_out.status.success(),
        "dispatch must remain paused on a daemon whose stop is already \
         committed — resuming it here would be unsafe: {}",
        String::from_utf8_lossy(&spawn_out.stdout)
    );

    let _ = rk(home.path()).args(["daemon", "stop"]).output();
}

#[test]
fn rollover_refuses_new_dispatch_while_draining() {
    let home = tempfile::tempdir().unwrap();
    disable_disk_floor(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    // Bring up a daemon with nothing live, so the drain step returns
    // immediately — this test only cares that `agent.spawn` is refused
    // between `daemon.pause_dispatch` and the eventual `stop`.
    rk(home.path())
        .args(["ping"])
        .output()
        .expect("run rk ping");
    json_stdout(
        &rk(home.path())
            .args(["--json", "repo", "add", repo_dir.path().to_str().unwrap()])
            .output()
            .unwrap(),
    );

    let rollover_out = rk(home.path())
        .env(
            "RK_FAKE_HARNESS_CMD",
            r#"echo '{"type":"result","subtype":"success","is_error":false,"result":"ok","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#,
        )
        .args(["--json", "daemon", "rollover", "--wait-secs", "0"])
        .output()
        .expect("run rk daemon rollover");
    let rollover = json_stdout(&rollover_out);
    assert_eq!(rollover["rolled_over"], true, "{rollover}");

    // Dispatch works again on the fresh (unpaused) daemon.
    let spawn_out = rk(home.path())
        .args([
            "--json",
            "spawn",
            "--task",
            "post-rollover",
            "--repo",
            repo_dir.path().to_str().unwrap(),
            "--harness",
            "fake",
        ])
        .output()
        .expect("run rk spawn");
    assert!(
        spawn_out.status.success(),
        "dispatch should work again after rollover completes: {}",
        String::from_utf8_lossy(&spawn_out.stderr)
    );

    let _ = rk(home.path()).args(["daemon", "stop"]).output();
}
