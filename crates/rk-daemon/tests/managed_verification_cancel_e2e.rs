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

mod fixture;
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
///
/// Committed, not just written: a live agent's `verify.run` resolves the
/// check from ITS OWN worktree (a real `git worktree` checkout), which only
/// sees committed content. The operator-direct-path test above doesn't need
/// the commit (it resolves the repo's registered root directly) but is
/// unaffected by having one.
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
    git(dir, &["add", ".rk/checks.cue"]);
    git(dir, &["commit", "-m", "test: install verify check"]);
}

/// rk-daemon doesn't own the `rk` binary, so cargo never sets
/// `CARGO_BIN_EXE_rk` for this test binary — fall back to the real target
/// dir, same resolution `foreman.rs` uses.
fn rk_bin() -> String {
    std::env::var("CARGO_BIN_EXE_rk").unwrap_or_else(|_| {
        let target_dir = std::env::var("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| support::workspace_root().join("target"));
        target_dir
            .join("debug")
            .join("rk")
            .to_string_lossy()
            .into_owned()
    })
}

/// A fake harness that issues a REAL `verify.run` call through the real `rk`
/// CLI binary (not a direct engine call) and holds its turn open so the test
/// can act on the live agent while that call is genuinely in flight. Waits
/// for the check's own pid file before sleeping, so a caller never proceeds
/// until the managed child has provably started.
fn hold_for_verify_script(rk: &str) -> String {
    format!(
        r#"
echo '{{"type":"system","subtype":"init","session_id":"cancel-e2e"}}'
read -r _prompt
'{rk}' verify --repo "$RK_REPO" > verify-rpc-output.txt 2>&1 &
for i in $(seq 1 200); do
  [ -f verify.pid ] && break
  sleep 0.05
done
sleep 30
"#,
        rk = rk
    )
}

/// Spawn a real fake-harness agent running [`hold_for_verify_script`],
/// returning its name and worktree.
async fn spawn_verify_holder(
    client: &mut Client,
    repo_dir: &Path,
    task: &str,
) -> (String, std::path::PathBuf) {
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.to_string_lossy(),
                "task": task,
                "role": "rat",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let worktree = std::path::PathBuf::from(spawned["agent"]["worktree"].as_str().unwrap());
    (agent, worktree)
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
            .call(
                "verify.run",
                json!({"repo": repo_for_call, "check": "verify"}),
            )
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

/// The terminal-death/dismiss half of item (2)
/// (TKT-01M0PBNGGZTNQPXB16214V4D7M): a REAL live agent spawn (fake harness)
/// issuing its OWN `verify.run` call via the real `rk` binary, dismissed
/// through the real `agent.dismiss` RPC while that call is genuinely in
/// flight — not `Supervisor::dismiss` called directly and not
/// `cancel_managed_verification_for_agent` invoked by hand. Proves
/// `Supervisor::dismiss`'s cancellation binding end to end, over the actual
/// spawn/RPC/harness machinery.
#[tokio::test]
async fn dismissing_a_live_agent_kills_its_own_in_flight_verify_run() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    install_verify_check(repo_dir.path());

    let rk = rk_bin();
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(&hold_for_verify_script(&rk)),
    );

    let mut client = start_daemon(&layout).await;
    let (agent, worktree) =
        spawn_verify_holder(&mut client, repo_dir.path(), "dismiss-cancel").await;

    let pid_path = worktree.join("verify.pid");
    let child_pid = wait_for_pid(&pid_path).await;
    assert!(
        process_alive(child_pid),
        "the agent's own verify run must have a real child alive before dismiss"
    );

    client
        .call("agent.dismiss", json!({"name": agent}))
        .await
        .unwrap();

    wait_for_death(child_pid).await;
}

/// Isolation half of item (4) (TKT-01M0PBNGGZTNQPXB16214V4D7M): two real
/// agents in two DIFFERENT repos each hold their own in-flight `verify.run`.
/// Dismissing one must cancel only its own managed child — the other repo's
/// run, genuinely concurrent and unrelated, must be left running untouched.
/// `cancel_managed_verification_for_agent` is scoped by agent name/spawn id,
/// not by repo, so this is the narrowest real proof that scoping actually
/// holds under two live agents rather than one.
#[tokio::test]
async fn dismissing_one_agent_does_not_touch_a_different_repos_in_flight_verify_run() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());

    let repo_a = tempfile::tempdir().unwrap();
    init_repo(repo_a.path());
    install_verify_check(repo_a.path());

    let repo_b = tempfile::tempdir().unwrap();
    init_repo(repo_b.path());
    install_verify_check(repo_b.path());

    let rk = rk_bin();
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(&hold_for_verify_script(&rk)),
    );

    let mut client = start_daemon(&layout).await;
    let (agent_a, worktree_a) =
        spawn_verify_holder(&mut client, repo_a.path(), "isolation-a").await;
    let (agent_b, worktree_b) =
        spawn_verify_holder(&mut client, repo_b.path(), "isolation-b").await;

    let pid_a = wait_for_pid(&worktree_a.join("verify.pid")).await;
    let pid_b = wait_for_pid(&worktree_b.join("verify.pid")).await;
    assert!(process_alive(pid_a));
    assert!(process_alive(pid_b));

    client
        .call("agent.dismiss", json!({"name": agent_a}))
        .await
        .unwrap();
    wait_for_death(pid_a).await;

    // A window this test does NOT need to pass — proof B's run is
    // genuinely unaffected, not merely "not dead yet" by luck of timing.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        process_alive(pid_b),
        "repo B's verify run must be unaffected by repo A's cancellation"
    );

    client
        .call("agent.dismiss", json!({"name": agent_b}))
        .await
        .unwrap();
    wait_for_death(pid_b).await;
}
