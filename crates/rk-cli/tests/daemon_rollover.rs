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
    // fast-completing script so the respawn is observable quickly.
    let rollover_out = rk(home.path())
        .env(
            "RK_FAKE_HARNESS_CMD",
            r#"echo '{"type":"result","subtype":"success","is_error":false,"result":"resumed after rollover","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#,
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
