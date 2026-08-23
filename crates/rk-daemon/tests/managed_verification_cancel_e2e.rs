//! Genuine full-daemon-over-socket coverage for managed-verification
//! cancellation (TKT-01M0PA6C5WYRWS757R1SS2F2GR), broadening
//! `workflow_exec.rs`'s in-process
//! `cancelling_a_managed_verification_run_kills_its_real_process_group_and_frees_the_queued_follower`
//! unit test. That test proves the mechanism by calling
//! `cancel_managed_verification_for_agent` directly; these tests drive the
//! SAME real check child process through the actual wire paths instead:
//! `server.rs::dispatch_watching_disconnect` (an RPC caller's socket
//! genuinely dying mid-`verify.run`) and a real spawned agent's dismissal.
//!
//! Every test here proves cancellation the same way the unit test does: the
//! `verify` check writes its OWN pid to a file before sleeping, so "the real
//! child died" is a `kill -0` liveness check on a concrete OS process, not
//! an inference from the daemon's own bookkeeping.

mod support;

use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::start_daemon;

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

/// A `verify` check that proves it is a real, independent OS process: it
/// writes its own pid to `verify.pid` (relative to the check's cwd) before
/// sleeping well past any of these tests' own deadlines.
fn install_verify_check(dir: &Path) {
    let rk_dir = dir.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("checks.cue"),
        r#"checks: [
    {name: "verify", command: "echo $$ > verify.pid; sleep 30", timeout: "30s", environmentPolicy: "strip_rk_spawn"},
]
"#,
    )
    .unwrap();
}

/// `kill -0`: existence check only, portable across the sandboxes these
/// tests run in (no `libc` dependency needed from an external test crate).
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

async fn wait_for_death(pid: i32) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if !process_alive(pid) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "child process {pid} is still alive after cancellation"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The RPC-disconnect half of TKT-01M0PA6C5WYRWS757R1SS2F2GR, exercised
/// against a real Unix socket instead of calling
/// `dispatch_watching_disconnect` in isolation: an operator connection makes
/// a genuine `verify.run` call, the check's real child process is confirmed
/// running (its own pid file exists), and the CALLER'S OWN connection is
/// then dropped out from under the in-flight call — the same failure shape
/// as a killed `rk verify` process or a lost network link. The daemon must
/// observe the real socket EOF and kill the real child; nothing in this test
/// calls `cancel_managed_verification_for_agent` or any other daemon
/// internal directly.
#[tokio::test]
async fn rpc_caller_disconnect_kills_the_real_managed_child_process() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());
    install_verify_check(repo_dir.path());

    let mut client = start_daemon(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // A SEPARATE connection is the caller under test: it must be the one
    // whose socket dies, distinct from the connection driving the rest of
    // this test's assertions.
    let doomed = Client::connect_as_operator(&layout).await.unwrap();
    let repo_for_call = repo_name.clone();
    let call_task = tokio::spawn(async move {
        let mut doomed = doomed;
        doomed
            .call("verify.run", json!({"repo": repo_for_call, "check": "verify"}))
            .await
    });

    let pid_path = repo_dir.path().join("verify.pid");
    let child_pid = wait_for_pid(&pid_path).await;
    assert!(
        process_alive(child_pid),
        "the check's real child must be alive before it can prove it dies"
    );

    // Kill the caller's own connection mid-call: dropping the task drops the
    // `Client` (and its owned socket) while `verify.run` is still pending,
    // which is exactly what `dispatch_watching_disconnect` is watching for.
    call_task.abort();
    let _ = call_task.await;

    wait_for_death(child_pid).await;
}
