//! Reactor end-to-end + guard tests. The live-daemon test proves the feed wakes
//! the reactor and a matching tuple fires a workflow. The direct-`Reactor` tests
//! drive `run_cycle` by hand — which is exactly "simulated feed loss": the feed
//! fired but no one consumed it, and the scan-driven cursor still dispatches —
//! and pin down idempotency, re-entrancy, and the per-trigger rate cap.

use rk_core::config::ReactorConfig;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Tuple};
use rk_daemon::reactor::{Reactor, REACTOR_INSTANCE};
use rk_daemon::repos::{RepoRecord, RepoRegistry};
use rk_daemon::supervisor::Supervisor;
use rk_daemon::tickets::Tickets;
use rk_daemon::workflow_exec::WorkflowEngine;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "reacted for $RK_TASK by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// A one-step workflow the reactor fires: spawn a fake rat. Enough to count
// dispatches (one workflow instance per fire).
const WORKFLOW: &str = r#"
workflow: {
    name: "react-work"
    agents: {default: {harness: "fake"}}
    steps: [
        {type: "spawn", role: "rat", task: {title: "reacted"}},
    ]
}
"#;

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
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    let wf_dir = dir.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("react-work.cue"), WORKFLOW).unwrap();
}

/// Register `myrepo` in the on-disk registry the reactor reads, so a tuple in
/// that scope resolves to a real checkout the fired workflow can run in.
fn register_repo(layout: &Layout, name: &str, path: &Path) {
    let mut reg = RepoRegistry::load(&layout.home().join("repos.json")).unwrap();
    reg.add(RepoRecord {
        name: name.into(),
        path: path.to_path_buf(),
        created_at: chrono::Utc::now(),
    })
    .unwrap();
}

/// Build a fully-wired reactor over the given in-memory space plus a real repo,
/// with whatever global trigger the test wrote: `event/myrepo/ping` fires
/// `react-work`.
fn build_reactor_with_space(
    layout: &Layout,
    config: ReactorConfig,
    space: rk_space::Space,
) -> Arc<Reactor> {
    let tickets = Arc::new(Tickets::new(space.clone(), "test-castle".into()));
    let supervisor = Arc::new(
        Supervisor::new(
            layout.clone(),
            "test-castle".into(),
            "fake".into(),
            rk_ledger::Budget::default(),
            space.clone(),
            tickets.clone(),
        )
        .unwrap(),
    );
    let engine = Arc::new(WorkflowEngine::new(
        layout.clone(),
        supervisor,
        space.clone(),
        tickets,
        Default::default(),
        "fake".into(),
    ));
    Arc::new(Reactor::new(space, engine, layout.clone(), config))
}

fn write_trigger(layout: &Layout) {
    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("ping.cue"),
        r#"triggers: [{name: "ping-drain", match: {category: "event", scope: "myrepo", identity: "ping"}, run: "react-work"}]"#,
    )
    .unwrap();
}

fn ping() -> Tuple {
    Tuple::new(
        Category::Event,
        "myrepo",
        "ping",
        "Whisker",
        json!({"n": 1}),
    )
}

/// Full path: the live daemon's reactor loop wakes off the feed and fires the
/// workflow when a matching tuple is written over the wire.
#[tokio::test]
async fn live_daemon_reactor_fires_workflow_on_matching_tuple() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout);

    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Register the repo, then write the tuple that should fire the workflow.
    client
        .call(
            "repo.add",
            json!({"name": "myrepo", "path": repo.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    client
        .call(
            "space.out",
            json!({"category": "event", "scope": "myrepo", "identity": "ping"}),
        )
        .await
        .unwrap();

    let mut fired = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let list = client.call("workflow.list", json!({})).await.unwrap();
        if list["instances"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false)
        {
            assert_eq!(list["instances"][0]["workflow"], "react-work");
            fired = true;
            break;
        }
    }
    assert!(fired, "reactor never fired the workflow");
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Idempotency under simulated feed loss: `run_cycle` is driven by hand (the
/// feed is never consumed), fires exactly once, and stays fired-once across a
/// repeat cycle AND a cursor reset (at-least-once redelivery) — the durable
/// marker is the guard, not the cursor.
#[tokio::test]
async fn dispatch_is_idempotent_under_feed_loss_and_cursor_reset() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout);
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // A matching tuple lands; nobody consumed the feed (loss). The scan fires it.
    space.out(ping()).unwrap();
    assert_eq!(reactor.run_cycle().unwrap(), 1, "first cycle fires once");
    // Re-running is a no-op: the cursor has advanced past it.
    assert_eq!(reactor.run_cycle().unwrap(), 0, "cursor prevents re-fire");

    // Simulate at-least-once redelivery: wipe the cursor so the tuple is seen
    // again. The durable marker must still suppress a second dispatch.
    std::fs::remove_file(home.path().join("reactor-cursor")).ok();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        0,
        "marker prevents re-fire after cursor loss"
    );

    // Exactly one workflow instance was ever created.
    assert_eq!(reactor.engine_instance_count(), 1);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Re-entrancy guard: the reactor never reacts to its own output, nor to
/// authors excluded by config or by the trigger.
#[tokio::test]
async fn reentrancy_and_exclusions_suppress_dispatch() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    // Trigger that also excludes the author "Nibbles".
    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("ping.cue"),
        r#"triggers: [{name: "ping-drain", match: {category: "event", scope: "myrepo", identity: "ping"}, run: "react-work", exclude: ["Nibbles"]}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        exclude_instances: vec!["daemon".into()],
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    // Authored by the reactor itself → ignored (tag guard).
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "ping",
            REACTOR_INSTANCE,
            json!({}),
        ))
        .unwrap();
    // Authored by a config-excluded instance → ignored.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "ping",
            "daemon",
            json!({}),
        ))
        .unwrap();
    // Authored by a trigger-excluded instance → ignored.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "ping",
            "Nibbles",
            json!({}),
        ))
        .unwrap();

    assert_eq!(
        reactor.run_cycle().unwrap(),
        0,
        "all excluded authors suppressed"
    );
    assert_eq!(reactor.engine_instance_count(), 0);

    // A non-excluded author still fires.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "ping",
            "Whisker",
            json!({}),
        ))
        .unwrap();
    assert_eq!(reactor.run_cycle().unwrap(), 1, "non-excluded author fires");
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Per-trigger rate cap: within one window the reactor fires at most `maxFires`
/// times regardless of how many matching tuples land — the storm backstop.
#[tokio::test]
async fn per_trigger_rate_cap_bounds_a_storm() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("ping.cue"),
        r#"triggers: [{name: "ping-drain", match: {category: "event", scope: "myrepo", identity: "ping"}, run: "react-work", maxFires: 2}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        window_secs: 3600, // one wide window for the whole test
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    // Five distinct matching tuples land, but the cap is 2.
    for i in 0..5 {
        space
            .out(Tuple::new(
                Category::Event,
                "myrepo",
                "ping",
                "Whisker",
                json!({"i": i}),
            ))
            .unwrap();
    }
    let fired = reactor.run_cycle().unwrap();
    assert_eq!(fired, 2, "rate cap bounds fires to maxFires");
    assert_eq!(reactor.engine_instance_count(), 2);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}
