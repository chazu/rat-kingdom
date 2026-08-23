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
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::{connect, start_daemon};

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
///
/// That fallback path is only ever populated as a side effect of building
/// the `rk-cli` package, which `cargo test -p rk-daemon --test ...` does NOT
/// do (rk-daemon has no build-time dependency on rk-cli). Without this
/// check, a stale/missing binary makes the fake harness's backgrounded
/// `'{rk}' verify ... &` fail invisibly, and these tests report the
/// downstream symptom 10 seconds later ("the check's real child never wrote
/// its own pid") instead of the real cause. `cargo test --workspace` (the
/// documented verification entrypoint) always builds it first, so this only
/// bites a scoped `-p rk-daemon` run against a fresh or partial target dir.
fn rk_bin() -> String {
    let path = std::env::var("CARGO_BIN_EXE_rk").unwrap_or_else(|_| {
        let target_dir = std::env::var("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| support::workspace_root().join("target"));
        target_dir
            .join("debug")
            .join("rk")
            .to_string_lossy()
            .into_owned()
    });
    assert!(
        Path::new(&path).exists(),
        "rk binary not found at {path} — these tests spawn the real `rk` CLI from a fake \
         harness script, but nothing in `cargo test -p rk-daemon` builds it. Build it first \
         (`cargo build -p rk-cli --bin rk`) or run `cargo test --workspace`, which builds \
         every workspace member including rk-cli."
    );
    path
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

/// Same real-`verify.run`-in-flight setup as [`hold_for_verify_script`], but
/// instead of holding the turn open, this declares `rk_done` and prints its
/// own `"type":"result"` line right away and lets the script end — the
/// harness process backing this generation exits on its own, with the
/// backgrounded `rk verify` call (and the check's real child, which sleeps
/// 30s) still genuinely in flight. Models item (2)'s
/// `HarnessEvent::Exited`-without-`interrupt`/`dismiss` case: a rat that
/// finishes and exits cleanly while its own completion-check run is still
/// mid-flight. Must be wrapped in [`fixture::with_rk_done`] so `rk_done` is
/// callable, exactly like [`hold_for_verify_script`]'s callers wrap it.
fn self_exit_after_verify_script(rk: &str) -> String {
    format!(
        r#"
echo '{{"type":"system","subtype":"init","session_id":"cancel-e2e"}}'
read -r _prompt
'{rk}' verify --repo "$RK_REPO" > verify-rpc-output.txt 2>&1 &
for i in $(seq 1 200); do
  [ -f verify.pid ] && break
  sleep 0.05
done
rk_done "self-exit-cancel"
echo '{{"type":"result","subtype":"success","is_error":false,"result":"self-exit-cancel","session_id":"cancel-e2e","total_cost_usd":0.001,"usage":{{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
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

/// Item (1): the real `agent.interrupt` RPC (`Supervisor::interrupt`,
/// `supervisor.rs` ~4166-4204) driving cancellation — only `dismiss` was
/// covered before this test. `interrupt`'s own SIGINT is delivered to the
/// harness's whole process group (`send_group_signal`, `rk-harness/src/
/// lib.rs`), which also reaches the harness's own `rk verify` CLI child
/// directly (same process group, backgrounded with `&`) — so this asserts on
/// the check's OWN independent child (`verify.pid`), which the daemon spawns
/// itself via `.process_group(0)` and therefore lives in a completely
/// separate process group untouched by that SIGINT. Only
/// `cancel_managed_verification_for_agent`'s `"agent_interrupt"` cancellation
/// path — not the SIGINT's own reach — can kill it.
#[tokio::test]
async fn interrupting_a_live_agent_kills_its_own_in_flight_verify_run() {
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
        spawn_verify_holder(&mut client, repo_dir.path(), "interrupt-cancel").await;

    let pid_path = worktree.join("verify.pid");
    let child_pid = wait_for_pid(&pid_path).await;
    assert!(
        process_alive(child_pid),
        "the agent's own verify run must have a real child alive before interrupt"
    );

    client
        .call("agent.interrupt", json!({"name": agent}))
        .await
        .unwrap();

    wait_for_death(child_pid).await;
}

/// Item (2): `HarnessEvent::Exited` firing with no explicit `interrupt` or
/// `dismiss` (`supervisor.rs` ~2342-2355, `"agent_terminal_death"`) — a
/// harness that declares `rk_done` and exits cleanly on its own while its own
/// `verify.run` is still genuinely in flight. Uses
/// [`self_exit_after_verify_script`], which never sleeps to hold the turn
/// open: the moment its own real child (behind the backgrounded `rk verify`
/// call) has started, it declares done, prints its result line, and lets the
/// script — and with it, the harness process itself — end. Proves the daemon
/// notices the generation is provably gone and cancels the child, not merely
/// that a deliberate operator action does.
#[tokio::test]
async fn a_harness_that_self_exits_after_declaring_done_kills_its_own_in_flight_verify_run() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    install_verify_check(repo_dir.path());

    let rk = rk_bin();
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(&self_exit_after_verify_script(&rk)),
    );

    let mut client = start_daemon(&layout).await;
    let (_agent, worktree) =
        spawn_verify_holder(&mut client, repo_dir.path(), "self-exit-cancel").await;

    let pid_path = worktree.join("verify.pid");
    let child_pid = wait_for_pid(&pid_path).await;
    assert!(
        process_alive(child_pid),
        "the agent's own verify run must have a real child alive before it self-exits"
    );

    // No RPC call here at all: the harness's own script does everything —
    // declares done, prints its result, and its process exits on its own.
    wait_for_death(child_pid).await;
}

/// Item (4)'s heavier variant: cross-repo isolation proven under FOUR
/// concurrent live agents (not just two), each in its own repo with its own
/// in-flight `verify.run`. Dismissing one at a time must cancel only that
/// one's managed child — the remaining, genuinely concurrent runs are
/// untouched — strengthening "under real concurrent load" beyond the
/// 2-agent proof above.
#[tokio::test]
async fn dismissing_one_of_several_concurrent_agents_only_cancels_its_own_verify_run() {
    const N: usize = 4;

    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());

    let repos: Vec<_> = (0..N)
        .map(|_| {
            let dir = tempfile::tempdir().unwrap();
            init_repo(dir.path());
            install_verify_check(dir.path());
            dir
        })
        .collect();

    let rk = rk_bin();
    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(&hold_for_verify_script(&rk)),
    );

    let mut client = start_daemon(&layout).await;
    let mut agents = Vec::with_capacity(N);
    for (i, repo) in repos.iter().enumerate() {
        let (agent, worktree) =
            spawn_verify_holder(&mut client, repo.path(), &format!("concurrent-{i}")).await;
        agents.push((agent, worktree));
    }

    let mut pids = Vec::with_capacity(N);
    for (_, worktree) in &agents {
        let pid = wait_for_pid(&worktree.join("verify.pid")).await;
        assert!(process_alive(pid));
        pids.push(pid);
    }

    // Dismiss agents one at a time, from the middle of the set outward, so a
    // survivor is checked both before and after its neighbors are cancelled
    // — not just the first and last in spawn order. `dismissed` tracks which
    // indices have already been cancelled by an earlier iteration, so those
    // are skipped rather than re-asserted as still alive.
    let mut dismissed = std::collections::HashSet::new();
    for &victim in &[2usize, 0, 3, 1] {
        client
            .call("agent.dismiss", json!({"name": agents[victim].0}))
            .await
            .unwrap();
        wait_for_death(pids[victim]).await;
        dismissed.insert(victim);
        for (i, &pid) in pids.iter().enumerate() {
            if dismissed.contains(&i) {
                continue;
            }
            assert!(
                process_alive(pid),
                "repo {i}'s verify run must be unaffected by repo {victim}'s cancellation"
            );
        }
    }
}

/// Item (3): what a daemon restart does to a `verify.run` that was
/// genuinely in flight when the previous daemon died. Uses the same
/// two-`Daemon`-over-one-on-disk-`Layout` pattern as `live_landing_restart
/// .rs`'s `two_daemon_restart_mid_gate_resumes_and_lands_through_the_reactor`
/// (`Daemon::new`, not `start_daemon`'s in-memory space, so state genuinely
/// persists across the two instances) — not reused directly because that
/// file's helpers are built around the landing pipeline, not a live agent's
/// own `verify.run`.
///
/// `ManagedVerificationRuns`'s own doc comment (`supervisor.rs`) says a
/// restart "drops every entry along with the managed child processes
/// themselves". Before this test's assertions were written, that was
/// measurably true only of the in-memory BOOKKEEPING, not of the OS-level
/// child PROCESS: `verify_repo_check`'s real check child is reached from a
/// per-connection task that `server.rs`'s accept loop spawns with a bare,
/// detached `tokio::spawn` — NOT the `background_tasks` `JoinSet` that
/// `Daemon::run`'s own doc comment explains exists specifically so aborting
/// the outer future tears down every task it owns. Aborting daemon A's
/// `run()` future (the same technique `live_landing_restart.rs` uses to
/// simulate a crash) never reaches that detached task, and neither would a
/// genuine `SIGKILL` of the whole process: the check child lives in ITS OWN
/// process group (`.process_group(0)`, deliberately so an operator
/// `interrupt`'s SIGINT does not reach it either), so an orphan of a killed
/// parent is reparented by the OS, not signalled.
///
/// `ManagedChildMarker`/`reap_stale_managed_children`
/// (`crates/rk-daemon/src/workflow_exec.rs`) close that gap: every check
/// child's pid is durably marked (with a `ps`-derived identity, not just the
/// bare pid, so a later restart can never mistake a pid the OS recycled for
/// an unrelated process as still being the one it spawned —
/// `reap_stale_managed_children_never_signals_a_pid_reused_by_an_unrelated_process`
/// pins that fail-closed behavior directly) the moment it is known, and
/// `Daemon::run` sweeps and kills every marker still on disk before its
/// accept loop can serve a single request. This test proves that sweep end
/// to end: daemon B genuinely kills the real child daemon A left running,
/// not merely a mock of the mechanism.
///
/// This test ALSO proves "cleanly" at the level that matters operationally
/// beyond the leaked process itself: the restart never leaves daemon B
/// blocked by daemon A's stale state. Daemon B's own `ManagedVerificationRuns`
/// and `VerificationAdmission` start genuinely empty (in-memory,
/// per-process), so nothing about the dead run's bookkeeping can hold its
/// repo's admission slot forever or deadlock a queued follower — daemon B
/// can immediately run its OWN fresh `verify.run` against the very same
/// repo. And the orphaned agent record is not left claiming to be live
/// forever: daemon B's startup (`Supervisor::on_daemon_started` ->
/// `orphan_live_agents`) marks it `orphaned`, a real terminal-ish state a
/// respawn sweep can act on, not `running`.
#[tokio::test]
async fn daemon_restart_never_blocks_progress_on_a_run_that_was_in_flight_when_it_died() {
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

    let config = rk_core::config::Config::default();

    // Daemon A: genuinely on-disk (`Daemon::new`), so its state is actually
    // inherited by daemon B below rather than starting from an empty store.
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let (agent, worktree) =
        spawn_verify_holder(&mut client, repo_dir.path(), "restart-in-flight").await;
    let pid_path = worktree.join("verify.pid");
    let child_pid = wait_for_pid(&pid_path).await;
    assert!(
        process_alive(child_pid),
        "the agent's own verify run must have a real child alive before the restart"
    );

    // The kill: abort the daemon's task outright, same technique
    // `live_landing_restart.rs` uses, then clear the stale pid/socket a real
    // crash would also leave with no live holder.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    // Daemon B: a fresh `Daemon::new` over the SAME on-disk home. Its own
    // startup, before this test's client can issue a single RPC, sweeps and
    // kills whatever daemon A left running (`reap_stale_managed_children`).
    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // The regression this item asked for: the REAL child daemon A left
    // running is genuinely dead, not merely presumed dead.
    wait_for_death(child_pid).await;

    let status = client
        .call("agent.status", json!({"name": &agent}))
        .await
        .unwrap();
    assert_eq!(
        status["agent"]["state"].as_str(),
        Some("orphaned"),
        "the dead generation's agent record must not still claim to be live: {status}"
    );

    // The forward-progress guarantee: daemon B is not blocked by ANY trace of
    // daemon A's in-flight run — it can immediately run its own fresh
    // `verify.run` against the very same repo.
    let (_second_agent, worktree_2) =
        spawn_verify_holder(&mut client, repo_dir.path(), "restart-in-flight-2").await;
    let child_pid_2 = wait_for_pid(&worktree_2.join("verify.pid")).await;
    assert!(
        process_alive(child_pid_2),
        "daemon B must be able to run its own fresh verify.run against the same repo, \
         unblocked by any trace of the dead run daemon A left behind"
    );

    // Best-effort cleanup of daemon B's own still-running check: not part of
    // the assertion, and would otherwise hold its `sleep 30` for the rest of
    // the suite's run (daemon B is about to be aborted, so its OWN sweep
    // will never run again to catch it).
    let _ = Command::new("kill")
        .args(["-9", &child_pid_2.to_string()])
        .status();

    handle_b.abort();
    let _ = handle_b.await;
}
