//! Seam 7 (strategic-review B5): nothing kills a harness process once it
//! reports a clean `rk done` — `sweep()` only ever looks at `is_live()`
//! agents, so a `Completed` record (and any process still running behind it
//! — interactive harnesses stay alive between turns) is invisible to the
//! runaway/stuck sweep forever. `Supervisor::schedule_done_kill` arms a
//! one-shot grace timer the moment a clean completion publishes: a process
//! still tracked after `done_kill_grace_secs` is SIGKILLed and the action is
//! announced through `RecoveryAnnouncer` (`recovery.rs`).

mod fixture;

use rk_core::config::SupervisorConfig;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
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

fn init_repo(dir: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    dir.file_name().unwrap().to_string_lossy().to_string()
}

/// One script, behaviour selected by `$RK_TASK`, because `RK_FAKE_HARNESS_CMD`
/// is process-global and tests in this binary run concurrently (same recipe
/// as `mid_flight_result.rs`'s `FAKE`).
///
/// - `lingers-after-done`: declares done, reports a clean result, then stays
///   alive well past any grace window under test — modeling an interactive
///   harness that does not exit between turns.
/// - `exits-after-done`: declares done, reports a clean result, and exits
///   immediately — the clean-exit path this seam must leave untouched.
const FAKE: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"donekill"}'
read -r _prompt
echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working on it"}],"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
rk_done "finished it"
result() { printf '{"type":"result","subtype":"success","is_error":false,"result":"%s","session_id":"donekill","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}\n' "$1"; }
case "$RK_TASK" in
  lingers-after-done)
    result "did the work"
    sleep 30
    ;;
  exits-after-done)
    result "did the work"
    ;;
esac
"#;

async fn spawn(
    home: &Path,
    repo_dir: &Path,
    task: &str,
    grace_secs: u64,
) -> (Client, String, String) {
    let repo_name = init_repo(repo_dir);
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(FAKE));
    let layout = Layout::at(home);
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_sweep_config(SupervisorConfig {
        done_kill_grace_secs: grace_secs,
        ..SupervisorConfig::default()
    });
    tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

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
    (client, repo_name, agent)
}

/// Poll `agent.status` until the record's state matches, returning it.
async fn await_state(client: &mut Client, agent: &str, state: &str) -> Value {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap();
        if status["agent"]["state"].as_str() == Some(state) {
            return status["agent"].clone();
        }
    }
    panic!("agent {agent} never reached state {state:?}");
}

fn pid_alive(pid: u64) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .unwrap()
        .success()
}

/// Every `recovery_action` event of `kind`, oldest first.
async fn recovery_actions(client: &mut Client, scope: &str, kind: &str) -> Vec<Value> {
    client
        .call(
            "space.scan",
            json!({
                "category": "event",
                "scope": scope,
                "identity": "recovery_action",
                "payload_search": format!("\"action_kind\":\"{kind}\""),
            }),
        )
        .await
        .unwrap()["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["payload"].clone())
        .collect()
}

/// The acceptance criterion: a harness lingering after `rk done` is dead
/// <=90s later. Grace is set tight (1s) so the test runs in seconds while
/// still exercising the real sleep-then-check code path.
#[tokio::test]
async fn lingering_harness_is_killed_after_grace() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let (mut client, repo, agent) =
        spawn(home.path(), repo_dir.path(), "lingers-after-done", 1).await;

    let record = await_state(&mut client, &agent, "completed").await;
    let pid = record["pid"]
        .as_u64()
        .expect("a lingering agent still has a pid");
    assert!(
        pid_alive(pid),
        "the fake harness sleeps 30s after `rk done` — it must still be alive right after completion"
    );

    // Poll well within the ticket's <=90s bound; grace is 1s so this settles
    // almost immediately in practice.
    let mut dead = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !pid_alive(pid) {
            dead = true;
            break;
        }
    }
    assert!(
        dead,
        "pid {pid} was not killed within 30s of a clean `rk done`"
    );

    // Kill announced via the recovery-announce helper: a durable
    // `recovery_action` event for `kill-process-group` was written.
    let actions = recovery_actions(&mut client, &repo, "kill-process-group").await;
    assert!(
        actions
            .iter()
            .any(|a| a["notice"]["subject"].as_str() == Some(agent.as_str())),
        "expected a kill-process-group recovery_action for {agent}: {actions:?}"
    );

    // Transcript flush survives the grace window: the turn's assistant text,
    // recorded before `rk done`, is still readable after the kill.
    let log = client
        .call("agent.log", json!({"name": agent}))
        .await
        .unwrap();
    let entries = log["entries"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.to_string().contains("working on it")),
        "expected the pre-completion transcript to survive the kill: {entries:?}"
    );
}

/// The clean-exit path is unaffected: a harness that declares done and exits
/// on its own within the grace window is never SIGKILLed, and no
/// `kill-process-group` recovery action is ever recorded for it.
#[tokio::test]
async fn clean_exit_after_done_is_not_touched() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    // A grace window long enough that, if this path were mistakenly treated
    // like the lingering one, the test would still catch it well before the
    // grace timer fires.
    let (mut client, repo, agent) =
        spawn(home.path(), repo_dir.path(), "exits-after-done", 5).await;

    // `AgentState::Completed` (as opposed to `Failed`) is itself the proof
    // this was a clean, declared finish — `is_error` lives on the published
    // `harness_result` event, not on the status record.
    await_state(&mut client, &agent, "completed").await;

    // Give the (inert, for this agent) grace timer time to fire and confirm
    // it still did nothing.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let actions = recovery_actions(&mut client, &repo, "kill-process-group").await;
    assert!(
        actions
            .iter()
            .all(|a| a["notice"]["subject"].as_str() != Some(agent.as_str())),
        "a harness that exited cleanly on its own must never be SIGKILLed: {actions:?}"
    );
}

/// B5 rework: a manual `rk respawn` of a `Completed` rat, issued during the
/// predecessor's grace window, must survive that predecessor's trailing
/// done-kill timer. `respawn_mode` intentionally reuses the record's
/// generation (`created_at`) so the transcript keeps appending to the same
/// file — but that means a generation-keyed grace check cannot tell the
/// predecessor process from its respawned successor, and the timer armed for
/// the first `rk done` SIGKILLs the second session's fresh process instead of
/// the (still-lingering, orphaned) first one. The fix keys the check on a
/// per-launch session token that a respawn overwrites, not on the generation.
///
/// Both generations run the same `lingers-after-done` script, so the
/// respawned session eventually gets a legitimate done-kill of its own (its
/// OWN timer, armed at its OWN completion) — this test only asserts survival
/// in the window after the PREDECESSOR's deadline but before the respawned
/// generation's own deadline, which is exactly the window the bug killed it
/// in.
#[tokio::test]
async fn respawn_during_grace_survives_predecessor_done_kill() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let grace_secs = 3;
    let (mut client, _repo, agent) = spawn(
        home.path(),
        repo_dir.path(),
        "lingers-after-done",
        grace_secs,
    )
    .await;

    let record = await_state(&mut client, &agent, "completed").await;
    let pid1 = record["pid"]
        .as_u64()
        .expect("the predecessor is still lingering right after completion");
    assert!(pid_alive(pid1), "predecessor must still be running");

    // Respawn partway through the predecessor's grace window, leaving margin
    // on both sides: before the predecessor's timer fires, and before the
    // respawned generation's own timer (armed at ITS completion, moments
    // from now) catches up to it.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let respawned = client
        .call("agent.respawn", json!({"name": agent}))
        .await
        .unwrap();
    let pid2 = respawned["agent"]["pid"]
        .as_u64()
        .expect("respawn must report a fresh pid");
    assert_ne!(pid1, pid2, "respawn must launch a distinct process");

    // Land past the predecessor's deadline (~3s after its completion, ~1.5s
    // from now) but before the respawned generation's own deadline (~3s after
    // ITS completion, ~1.5s+grace from now) — the window the bug killed pid2
    // in.
    tokio::time::sleep(Duration::from_millis(2200)).await;

    assert!(
        pid_alive(pid2),
        "the respawned session must survive the predecessor's done-kill grace timer"
    );

    let _ = Command::new("kill")
        .args(["-9", &pid1.to_string()])
        .status();
    let _ = Command::new("kill")
        .args(["-9", &pid2.to_string()])
        .status();
}
