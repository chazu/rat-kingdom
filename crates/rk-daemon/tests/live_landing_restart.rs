//! Genuinely live two-`Daemon`-process restart, through the reactor's
//! `action: "land"` trigger dispatch end to end — not `LandingPipeline`
//! methods called directly (those are `pub(crate)`, which is why this was
//! split out rather than folded into `crates/rk-daemon/src/landing.rs`'s own
//! tests). Split from TKT-01M050VVGB7DVVTQ656EV20YPE item (1).
//!
//! `landing.rs`'s `restart_mid_gate_run_resumes_and_lands` and
//! `park_and_resume_survives_space_level_restart_with_late_verdict` already
//! prove the pipeline's restart-safety at the `Space`-reopen level, driven
//! directly against a `LandingPipeline` the test builds by hand. This test
//! proves the same guarantee end to end: the completion that seeds the
//! candidate comes from a real fake-harness rat spawned over the wire, the
//! trigger dispatch that enqueues it is the live reactor's `action: "land"`
//! path (`crates/rk-daemon/src/reactor.rs::fire_land_action`), and the
//! daemon carrying the mid-gate candidate is a genuine `Daemon::new` over an
//! on-disk layout that gets killed and replaced by a second `Daemon::new`
//! over the SAME home — nothing here calls into `rk-daemon`'s internals
//! directly.
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

mod fixture;

/// The daemon-native landing pipeline's completion feed
/// (`examples/triggers-landing-pipeline.cue`'s `steward-landing-on-completion`,
/// reproduced here rather than read from that file so this test does not
/// depend on the example's path or content staying byte-identical).
const LANDING_TRIGGER: &str = r#"
triggers: [
    {
        name:   "landing-on-completion"
        action: "land"
        match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
        maxFires: 20
    },
]
"#;

/// The two policy gates pass instantly; `verify` sleeps just long enough to
/// give the "kill mid-gate" step below a real window in which the candidate
/// is genuinely `running_gates`, not merely queued — same shape as
/// `landing.rs`'s own `restart_mid_gate_run_resumes_and_lands`.
const CHECKS: &str = r#"
checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "sleep 0.6 && true", timeout: "30s"},
]
"#;

/// A doc-only change: `classify_diff` (`crates/rk-daemon/src/supervisor.rs`)
/// routes it straight to `Supervisor::land` on a gate pass, no reviewer
/// needed — keeping this test's only variable the restart, not review
/// tiering.
const WORKING_FAKE: &str = r#"
read -r _prompt
mkdir -p docs
echo "note" > docs/note.md
git add docs/note.md >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "docs: add note"
echo '{"type":"system","subtype":"init","session_id":"land-restart-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"land-restart-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

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

/// `.rk/checks.cue` is committed on `main` before any branch forks off it, so
/// both the rat's own branch and the daemon's detached gate worktree carry it
/// (`landing.rs`'s `write_checks` doc comment explains why commit order
/// matters here).
fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);

    let rk_dir = dir.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), CHECKS).unwrap();
    git(dir, &["add", ".rk/checks.cue"]);
    git(dir, &["commit", "-m", "add checks registry"]);
}

fn test_config() -> rk_core::config::Config {
    let mut config = rk_core::config::Config::default();
    config.harness.default = "fake".into();
    config
}

async fn wait_agent_completed(client: &mut Client, name: &str) {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        match status["agent"]["state"].as_str() {
            Some("completed") => return,
            Some("failed") => panic!("rat failed instead of completing: {status}"),
            _ => {}
        }
    }
    panic!("rat never completed");
}

/// The current durable `landing_queue_entry` tuple for `repo_name`, if any —
/// read purely over `space.scan` RPC, the same surface `rk scan` gives an
/// operator, never `rk-daemon`'s internal `LandingQueue`.
async fn queue_entry(client: &mut Client, repo_name: &str) -> Option<serde_json::Value> {
    let res = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": repo_name, "identity": "landing_queue_entry"}),
        )
        .await
        .unwrap();
    res["tuples"].as_array().and_then(|a| a.first()).cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_daemon_restart_mid_gate_resumes_and_lands_through_the_reactor() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let main_before = git(repo_dir.path(), &["rev-parse", "main"]);

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::write(layout.triggers_dir().join("landing.cue"), LANDING_TRIGGER).unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let config = test_config();

    // Daemon A: a genuinely on-disk `Space` (`Daemon::new`, not
    // `new_in_memory`) — the whole point being that a second `Daemon::new`
    // below actually inherits this one's durable state rather than starting
    // from an empty store.
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "add docs note",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let agent_name = spawned["agent"]["name"].as_str().unwrap().to_string();
    wait_agent_completed(&mut client, &agent_name).await;

    // The rat's completion wrote a `harness_result` event; the live reactor's
    // `action: "land"` trigger matched it and enqueued straight onto the
    // daemon-native `LandingQueue` — no workflow ever ran, and this test
    // never touched `LandingPipeline` itself. Poll until the background
    // consumer has genuinely claimed the candidate and is mid-`verify`
    // (the check's `sleep 0.6` is what makes that window real), then kill
    // the daemon while that future is still in flight.
    let mut mid_gate = false;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Some(entry) = queue_entry(&mut client, &repo_name).await {
            if entry["payload"]["status"] == "running_gates" {
                mid_gate = true;
                break;
            }
        }
    }
    assert!(
        mid_gate,
        "candidate never reached running_gates before the kill"
    );

    // The kill: abort the daemon's task outright rather than the graceful
    // `stop` RPC, so the in-flight gate run is genuinely cut off mid-flight
    // instead of being allowed to finish first (the landing consumer loop's
    // shutdown signal is only checked BETWEEN cycles — a graceful stop would
    // let an already-started `run_cycle` finish). `handle.abort()` still
    // drops the singleton flock (an OS-level lock tied to the fd, released
    // on drop regardless of how the task ends), but this test process stays
    // alive throughout — unlike a real crash, it never leaves the pid dead —
    // so the stale pid file and socket a real crash would also leave behind
    // with no live holder are cleared by hand before the second daemon binds.
    handle_a.abort();
    let _ = handle_a.await;
    std::fs::remove_file(layout.pid_file()).ok();
    std::fs::remove_file(layout.socket_path()).ok();

    // The candidate survived the kill, durably `running_gates` — proof the
    // kill landed genuinely mid-gate rather than after the pipeline had
    // already finished and moved on to draining the entry.
    {
        let space = rk_space::Space::open(&layout.db_path()).unwrap();
        let pending = space
            .scan(
                &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                    .identity("landing_queue_entry"),
            )
            .unwrap();
        assert_eq!(pending.len(), 1, "candidate must survive the kill");
        assert_eq!(pending[0].payload["status"], "running_gates");
    }

    // Daemon B: a fresh `Daemon::new` over the SAME on-disk home, with the
    // SAME installed trigger file rediscovered off disk (not carried over in
    // this test process). Its reactor cursor and the `LandingQueue` are read
    // straight off the same `Space`.
    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    let mut landed = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if queue_entry(&mut client, &repo_name).await.is_none() {
            landed = true;
            break;
        }
    }
    assert!(
        landed,
        "landing queue entry never drained after the restart"
    );

    let main_after = git(repo_dir.path(), &["rev-parse", "main"]);
    assert_ne!(
        main_before, main_after,
        "the branch must have landed onto main after the restart"
    );
    assert!(
        git(repo_dir.path(), &["show", "main:docs/note.md"]).contains("note"),
        "the rat's doc-only work must be on main"
    );

    // This candidate carries no `--task` (a bare named-branch land) and
    // survived a daemon restart mid-flight — proving both that automatic
    // finalization derives the agent's merge pointer for a non-ticket
    // delivery, and that the derivation itself is restart/replay-safe.
    let status = client
        .call("agent.status", json!({"name": &agent_name}))
        .await
        .unwrap();
    assert_eq!(
        status["agent"]["merge_commit"].as_str(),
        Some(main_after.as_str()),
        "the rat's own generation must carry the merge pointer it landed"
    );

    handle_b.abort();
    let _ = handle_b.await;
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
