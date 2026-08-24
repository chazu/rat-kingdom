//! Gap 1 of TKT-01M0HNE2FYHYS5HCDW618VRQJD: `post_commit_recovery_rpc.rs`
//! proves the wire and durability for the generic fake harness, but the
//! alternate-harness-continuation and daemon-restart-mid-recovery seams were
//! only ever exercised in-process against the pure `Supervisor` (see
//! `continue_recovery_routes_to_a_configured_alternate_harness` and
//! `abandoned_recovery_stays_excluded_from_respawn_sweep_forever` in
//! `supervisor.rs`). This file proves the missing wire/restart boundary on
//! real `RK_CLAUDE_BIN`/`RK_CODEX_BIN` adapters, mirroring the kill/replace
//! daemon pattern from `transport_outage.rs`. It does not re-derive the
//! outcome logic those unit tests already cover.

mod support;

use rk_core::config::SupervisorConfig;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_ledger::Budget;
use rk_space::Space;
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// Both tests in this file set the process-global `RK_CLAUDE_BIN`/
/// `RK_CODEX_BIN` env vars (mirrors `transport_outage.rs`'s
/// `HARNESS_ENV_LOCK`) — without this, cargo's default concurrent test
/// execution lets one test's fixture path clobber the other's mid-run.
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# recovery continuation fixture\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

fn sweep_config() -> SupervisorConfig {
    SupervisorConfig {
        enabled: true,
        interval_secs: 1,
        stuck_after_secs: 0,
        burn_usd_per_min: 0.0,
        kill_grace_secs: 1,
        respawn_enabled: false,
        ..SupervisorConfig::default()
    }
}

/// Unlike `sweep_config`, auto-respawn is ON with no backoff — the restarted
/// daemon's own background sweep loop gets real, repeated chances to
/// resurrect a Failed record within the test's short sleep window. That is
/// the point: proving an abandoned recovery's exclusion holds under a live
/// sweep loop across a restart, not only when a test calls
/// `Supervisor::respawn_sweep` by hand in-process.
fn respawn_sweep_config() -> SupervisorConfig {
    SupervisorConfig {
        enabled: true,
        interval_secs: 1,
        stuck_after_secs: 0,
        burn_usd_per_min: 0.0,
        kill_grace_secs: 1,
        respawn_enabled: true,
        respawn_max_attempts: 3,
        respawn_backoff_secs: 0,
        ..SupervisorConfig::default()
    }
}

fn daemon(layout: &Layout) -> Daemon {
    let space = Space::open(&layout.db_path()).unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "claude".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_sweep_config(sweep_config());
    daemon
}

fn daemon_with_respawn(layout: &Layout) -> Daemon {
    let space = Space::open(&layout.db_path()).unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "claude".into(),
        Budget::default(),
        space,
    )
    .unwrap();
    daemon.set_sweep_config(respawn_sweep_config());
    daemon
}

/// Unlike `daemon()`'s `Budget::default()` (unlimited — `max_usd == 0.0`
/// means `detect_post_commit_outage` never populates
/// `recovery.budget_remaining_usd` at all, see `Supervisor::budget_for`),
/// this gives the recovery-budget-preservation proof a real, nonzero cap so
/// there is an actual remaining-budget figure to preserve across the restart.
fn daemon_with_budget(layout: &Layout, budget: Budget) -> Daemon {
    let space = Space::open(&layout.db_path()).unwrap();
    let mut daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "claude".into(),
        budget,
        space,
    )
    .unwrap();
    daemon.set_sweep_config(sweep_config());
    daemon
}

/// `rk-daemon` doesn't build the `rk` binary itself (no build-time
/// dependency on `rk-cli`), so `cargo test -p rk-daemon` alone never
/// populates `CARGO_BIN_EXE_rk` — same fallback
/// `managed_verification_cancel_e2e.rs`/`verification_saturation_regression.rs`
/// use.
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
        "rk binary not found at {path} — build it first (`cargo build -p rk-cli --bin rk`) or \
         run `cargo test --workspace`, which builds every workspace member including rk-cli."
    );
    path
}

/// Drive the real `rk` CLI as the operator against `home`'s daemon — strips
/// the full spawn-identity env (not just `RK_AGENT`), so this rat's own
/// ambient identity can never leak into what must authenticate as the
/// operator `agent.continue_recovery`/`agent.abandon_recovery` require.
fn rk(home: &Path) -> Command {
    let mut cmd = Command::new(rk_bin());
    for var in rk_core::review::STRIPPED_RK_SPAWN_ENV {
        cmd.env_remove(var);
    }
    cmd.env("RK_HOME", home);
    cmd
}

fn json_stdout(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

fn marker_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().count())
        .unwrap_or(0)
}

async fn wait_for_status(
    client: &mut Client,
    name: &str,
    predicate: impl Fn(&Value) -> bool,
    description: &str,
) -> Value {
    for _ in 0..600 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if predicate(&status) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {description}");
}

/// A real Claude adapter child that reaches its `Started` handshake (so the
/// pre-work transport watcher never fires — `detect_post_commit_outage` would
/// otherwise be shadowed by the pre-work `TransportFailure` path, see
/// `rk_harness::watch_pre_work_transport_failure`), commits work in its
/// worktree, then dies on a transport-classified stderr signal.
const CLAUDE_POST_COMMIT_OUTAGE: &str = r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"pre-outage-session"}'
git config user.email rat@example.com
git config user.name Rat
echo work > delivered.txt
git add delivered.txt
git commit -q -m 'committed work before the outage'
printf '%s\n' 'fatal: connection refused while contacting api' >&2
exit 1
"#;

/// A real Codex adapter child that reaches its own `Started` handshake
/// (`thread.started`) and exits cleanly — the alternate-harness continuation
/// target.
const CODEX_CONTINUATION: &str = r#"#!/bin/sh
printf '%s\n' '{"type":"thread.started","thread_id":"continued-session"}'
exit 0
"#;

/// Same shape as [`CLAUDE_POST_COMMIT_OUTAGE`], but counts its own
/// invocations to a marker file so a test can prove an abandoned generation
/// is never relaunched — across a real, restarted daemon's live sweep loop,
/// not just a single in-process `respawn_sweep` call.
fn claude_post_commit_outage_with_marker(marker: &Path) -> String {
    format!(
        r#"#!/bin/sh
printf 'attempt\n' >> '{}'
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"pre-outage-session"}}'
git config user.email rat@example.com
git config user.name Rat
echo work > delivered.txt
git add delivered.txt
git commit -q -m 'committed work before the outage'
printf '%s\n' 'fatal: connection refused while contacting api' >&2
exit 1
"#,
        marker.display()
    )
}

/// Same-provider continuation fixture: behaves as
/// [`CLAUDE_POST_COMMIT_OUTAGE`] (commit work, then die on a transport
/// signal) on its FIRST invocation, but also reports token usage first, so
/// `record.cost_usd` is genuinely nonzero before the outage is detected —
/// the budget-preservation proof needs a real accumulated spend to
/// preserve, not just an unlimited/zero budget. Every invocation AFTER the
/// first is the resumed session instead: it records its own argv (proving
/// `--resume <original-session-id>` was actually passed, i.e. the ORIGINAL
/// session was targeted) to `argv_capture`, then BLOCKS on `release`
/// (polled, same technique as `managed_verification_cancel_e2e.rs`'s
/// `hold_for_verify_script`) before reporting its own `Started` handshake
/// and exiting cleanly with no further usage/cost.
///
/// The block matters: `Supervisor::handle_event`'s `Started` arm clears the
/// WHOLE parked `recovery` record — ack included — the instant a continued
/// generation proves it is alive (by design: a live generation no longer
/// needs a parked-continuation record). A caller replaying `action_id` is
/// only guaranteed the recorded outcome while that record still stands, so
/// the replay proof below must run BEFORE this fixture is allowed to speak,
/// not after.
///
/// One script plays both parts because `RK_CLAUDE_BIN` is resolved once
/// from this process's env (`ClaudeHarness::launch`) and reused verbatim by
/// `continue_recovery`'s relaunch — a same-provider resume is never handed
/// a different binary path than the original spawn.
fn claude_same_provider_resume_fixture(
    marker: &Path,
    argv_capture: &Path,
    release: &Path,
) -> String {
    format!(
        r#"#!/bin/sh
printf 'attempt\n' >> '{marker}'
count=$(wc -l < '{marker}' | tr -d ' ')
if [ "$count" = "1" ]; then
  printf '%s\n' '{{"type":"system","subtype":"init","session_id":"pre-outage-session"}}'
  printf '%s\n' '{{"type":"assistant","message":{{"role":"assistant","content":[],"usage":{{"input_tokens":10000,"output_tokens":5000,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}}}'
  git config user.email rat@example.com
  git config user.name Rat
  echo work > delivered.txt
  git add delivered.txt
  git commit -q -m 'committed work before the outage'
  printf '%s\n' 'fatal: connection refused while contacting api' >&2
  exit 1
else
  printf '%s\n' "$*" > '{argv_capture}'
  for i in $(seq 1 200); do
    [ -f '{release}' ] && break
    sleep 0.05
  done
  printf '%s\n' '{{"type":"system","subtype":"init","session_id":"resumed-session"}}'
  exit 0
fi
"#,
        marker = marker.display(),
        argv_capture = argv_capture.display(),
        release = release.display(),
    )
}

/// Checkpoint scenario for gap 1(a)/(c): a post-commit recovery parked by a
/// real Claude adapter is continued under a real, configured alternate
/// harness (Codex) over RPC, and the parked record — plus the at-most-once
/// `action_id` ack it eventually carries — survives a daemon restart
/// injected between park and continuation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_recovery_routes_to_a_real_alternate_harness_across_a_daemon_restart() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let claude = home.path().join("claude-fixture");
    executable(&claude, CLAUDE_POST_COMMIT_OUTAGE);
    let codex = home.path().join("codex-fixture");
    executable(&codex, CODEX_CONTINUATION);
    std::env::set_var("RK_CLAUDE_BIN", &claude);
    std::env::set_var("RK_CODEX_BIN", &codex);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let handle_a = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "post-commit-outage",
                "harness": "claude",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let original_spawn = spawned["agent"]["spawn"].clone();

    let parked = wait_for_status(
        &mut client,
        &name,
        |s| !s["agent"]["recovery"].is_null(),
        "a real post-commit outage to park a durable recovery record",
    )
    .await;
    assert_eq!(
        parked["agent"]["recovery"]["ack"],
        Value::Null,
        "nothing has acted on it yet"
    );
    assert_eq!(parked["agent"]["recovery"]["provider"], "claude");

    // Restart the daemon BETWEEN detection and continuation: the record this
    // relies on must be a durable seam, not an in-memory one owned by the
    // detecting process (mirrors transport_outage.rs's kill/replace pattern).
    client.call("stop", json!({})).await.unwrap();
    handle_a.await.unwrap().unwrap();
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let handle_b = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;

    let response = client
        .call(
            "agent.continue_recovery",
            json!({"name": name, "action_id": "alt-1", "harness": "codex"}),
        )
        .await
        .unwrap();
    let outcome = response["outcome"].clone();
    assert_eq!(outcome["ContinuedAlternateProvider"]["harness"], "codex");
    assert_eq!(
        outcome["ContinuedAlternateProvider"]["new_spawn"], original_spawn,
        "continuation must preserve the original generation's identity"
    );

    // At-most-once across the restart boundary: replaying the SAME
    // action_id must return the recorded outcome, not launch a second real
    // Codex process.
    let replay = client
        .call(
            "agent.continue_recovery",
            json!({"name": name, "action_id": "alt-1", "harness": "codex"}),
        )
        .await
        .unwrap();
    assert_eq!(
        replay["outcome"], outcome,
        "replaying the action_id must return the recorded outcome, not re-act"
    );

    let settled = wait_for_status(
        &mut client,
        &name,
        |s| {
            !matches!(
                s["agent"]["state"].as_str(),
                Some("spawning" | "running" | "paused")
            )
        },
        "the real Codex continuation to settle",
    )
    .await;
    assert_eq!(
        settled["agent"]["harness"], "codex",
        "the record must reflect the alternate harness it actually continued under"
    );

    client.call("stop", json!({})).await.unwrap();
    handle_b.await.unwrap().unwrap();
    std::env::remove_var("RK_CLAUDE_BIN");
    std::env::remove_var("RK_CODEX_BIN");
}

/// Gap 1(b): `abandoned_recovery_stays_excluded_from_respawn_sweep_forever`
/// (supervisor.rs) proves the exclusion by calling `respawn_sweep` once,
/// in-process, against a hand-built record. This proves the same terminal
/// outcome — WIP release stays permanent, the harness never relaunches — over
/// RPC on a real Claude adapter, under a RESTARTED daemon whose own
/// background sweep loop is live and auto-respawn-enabled, so it gets
/// several real, unmocked chances to resurrect the generation and must not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_recovery_stays_terminal_across_a_restart_under_a_live_respawn_sweep() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let marker = home.path().join("claude-attempts");
    let claude = home.path().join("claude-fixture");
    executable(&claude, &claude_post_commit_outage_with_marker(&marker));
    std::env::set_var("RK_CLAUDE_BIN", &claude);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let handle_a = tokio::spawn(daemon(&layout).run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "post-commit-outage-abandon",
                "harness": "claude",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    wait_for_status(
        &mut client,
        &name,
        |s| !s["agent"]["recovery"].is_null(),
        "a real post-commit outage to park a durable recovery record",
    )
    .await;
    assert_eq!(marker_count(&marker), 1);

    let abandoned = client
        .call(
            "agent.abandon_recovery",
            json!({"name": name, "action_id": "give-up-1"}),
        )
        .await
        .unwrap();
    assert_eq!(abandoned["outcome"], "Abandoned");

    // Restart into a daemon whose sweep config actually auto-respawns Failed
    // agents (unlike `daemon()`'s config, used above only to detect the
    // outage without any respawn interference).
    client.call("stop", json!({})).await.unwrap();
    handle_a.await.unwrap().unwrap();
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let handle_b = tokio::spawn(daemon_with_respawn(&layout).run());
    let mut client = connect(&layout).await;

    // Cross several real sweep ticks (1s interval, 0 backoff) and prove none
    // of them resurrect it.
    tokio::time::sleep(Duration::from_millis(3_500)).await;
    let after = client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(
        after["agent"]["state"], "failed",
        "an abandoned recovery must never be auto-respawned, even under a live sweep loop \
         after a restart"
    );
    assert!(
        after["agent"]["pid"].is_null(),
        "WIP must stay released permanently"
    );
    assert_eq!(after["agent"]["recovery"]["ack"]["outcome"], "Abandoned");
    assert_eq!(
        marker_count(&marker),
        1,
        "the abandoned generation must never relaunch its harness"
    );

    client.call("stop", json!({})).await.unwrap();
    handle_b.await.unwrap().unwrap();
    std::env::remove_var("RK_CLAUDE_BIN");
}

/// The same-provider half of gap 1: the alternate-harness test above proves
/// `target_harness: Some(...)`; this proves `target_harness: None` — the
/// SAME session resumed under the SAME provider — over the real `rk
/// continue-recovery` CLI (not raw RPC), with a nonzero agent budget and
/// accumulated cost at the moment of outage, a daemon restart injected
/// between detection and the operator's action, and the recovery budget
/// checked byte-for-byte at every hop: detection, restart, continuation,
/// and replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_recovery_resumes_the_same_provider_across_a_restart_with_budget_preserved() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    let marker = home.path().join("claude-attempts");
    let argv_capture = home.path().join("resume-argv.txt");
    let release = home.path().join("resume-release");
    let claude = home.path().join("claude-fixture");
    executable(
        &claude,
        &claude_same_provider_resume_fixture(&marker, &argv_capture, &release),
    );
    std::env::set_var("RK_CLAUDE_BIN", &claude);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let budget = Budget {
        max_usd: 50.0,
        max_tokens: 0,
        warn_at: 0.8,
    };
    let handle_a = tokio::spawn(daemon_with_budget(&layout, budget).run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "post-commit-outage-same-provider",
                "harness": "claude",
                "model": "sonnet",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let original_spawn = spawned["agent"]["spawn"].clone();

    let parked = wait_for_status(
        &mut client,
        &name,
        |s| !s["agent"]["recovery"].is_null(),
        "a real post-commit outage to park a durable recovery record with accumulated cost",
    )
    .await;
    assert_eq!(marker_count(&marker), 1);
    // sonnet pricing (vendored table): $3e-6/input token, $15e-6/output token.
    let expected_cost = 10_000.0 * 3e-6 + 5_000.0 * 15e-6;
    assert_eq!(
        parked["agent"]["cost_usd"].as_f64().unwrap(),
        expected_cost,
        "the harness-reported usage must have accumulated real cost before the outage"
    );
    let expected_remaining = 50.0 - expected_cost;
    assert_eq!(
        parked["agent"]["recovery"]["budget_remaining_usd"]
            .as_f64()
            .unwrap(),
        expected_remaining,
        "budget_remaining_usd must reflect the accumulated cost at the moment of detection"
    );
    assert_eq!(parked["agent"]["recovery"]["provider"], "claude");
    assert_eq!(parked["agent"]["recovery"]["ack"], Value::Null);

    // Restart the daemon BETWEEN detection and the operator's action — the
    // budget snapshot this relies on must be durable, not in-memory.
    client.call("stop", json!({})).await.unwrap();
    handle_a.await.unwrap().unwrap();
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    let handle_b = tokio::spawn(daemon_with_budget(&layout, budget).run());
    let mut client = connect(&layout).await;

    let after_restart = client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(
        after_restart["agent"]["cost_usd"].as_f64().unwrap(),
        expected_cost,
        "accumulated cost must survive the restart exactly"
    );
    assert_eq!(
        after_restart["agent"]["recovery"]["budget_remaining_usd"]
            .as_f64()
            .unwrap(),
        expected_remaining,
        "the durable recovery record's budget snapshot must survive the restart byte-for-byte"
    );

    // Same-provider continuation over the REAL `rk continue-recovery` CLI —
    // no `--harness` override, so this resumes rather than routing to an
    // alternate provider.
    let out = rk(home.path())
        .args([
            "--json",
            "continue-recovery",
            &name,
            "--action-id",
            "same-provider-resume-1",
        ])
        .output()
        .unwrap();
    let response = json_stdout(&out);
    assert_eq!(
        response["ResumedSameProvider"]["new_spawn"], original_spawn,
        "same-provider continuation must preserve the original generation's identity"
    );

    // `continue_recovery` returns as soon as the relaunch is issued — the
    // resumed fixture is still blocked on `release` at this point, so the
    // parked `recovery` record (ack included) is guaranteed to still stand.
    // Both the at-most-once replay and the budget-preservation checks below
    // run against that still-parked record on purpose: once the resumed
    // generation is allowed to speak, its `Started` handshake clears
    // `recovery` entirely (by design — a live generation no longer needs a
    // parked-continuation record), so this window is the only place either
    // check can be made.
    let after_continue = client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(
        after_continue["agent"]["recovery"]["budget_remaining_usd"]
            .as_f64()
            .unwrap(),
        expected_remaining,
        "the recovery record's budget snapshot must be untouched by a successful continuation"
    );

    // At-most-once, over the real CLI: replaying with the SAME action_id
    // must return the recorded outcome, not relaunch a second real process.
    let replay = rk(home.path())
        .args([
            "--json",
            "continue-recovery",
            &name,
            "--action-id",
            "same-provider-resume-1",
        ])
        .output()
        .unwrap();
    let replay_response = json_stdout(&replay);
    assert_eq!(
        replay_response, response,
        "replaying the action_id over the CLI must return the recorded outcome, not re-act"
    );
    assert_eq!(
        marker_count(&marker),
        2,
        "the replayed action_id must not launch a second real Claude process"
    );
    let after_replay = client
        .call("agent.status", json!({"name": name}))
        .await
        .unwrap();
    assert_eq!(
        after_replay["agent"]["recovery"]["budget_remaining_usd"]
            .as_f64()
            .unwrap(),
        expected_remaining,
        "replaying the action_id must not move the recovery record's budget snapshot either"
    );

    // Release the resumed fixture now that both checks against the parked
    // record are done — it proceeds to its own `Started` handshake, which
    // clears `recovery` and lets the generation settle normally.
    std::fs::write(&release, "go").unwrap();

    let settled = wait_for_status(
        &mut client,
        &name,
        |s| {
            !matches!(
                s["agent"]["state"].as_str(),
                Some("spawning" | "running" | "paused")
            )
        },
        "the real same-provider resume to settle",
    )
    .await;
    assert_eq!(
        settled["agent"]["harness"], "claude",
        "a same-provider continuation must not change the recorded harness"
    );
    assert_eq!(
        settled["agent"]["cost_usd"].as_f64().unwrap(),
        expected_cost,
        "a same-provider resume must not silently grant a fresh budget window"
    );
    assert_eq!(
        marker_count(&marker),
        2,
        "exactly one real resume launch, on top of the one original outage launch, even once \
         the resumed generation has fully settled"
    );
    let argv = std::fs::read_to_string(&argv_capture).unwrap();
    assert!(
        argv.contains("--resume pre-outage-session"),
        "the resumed launch must target the ORIGINAL session, not a fresh one: {argv}"
    );

    client.call("stop", json!({})).await.unwrap();
    handle_b.await.unwrap().unwrap();
    std::env::remove_var("RK_CLAUDE_BIN");
}
