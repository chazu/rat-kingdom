//! Reactor end-to-end + guard tests. The live-daemon test proves the feed wakes
//! the reactor and a matching tuple fires a workflow. The direct-`Reactor` tests
//! drive `run_cycle` by hand — which is exactly "simulated feed loss": the feed
//! fired but no one consumed it, and the scan-driven cursor still dispatches —
//! and pin down idempotency, re-entrancy, and the per-trigger rate cap.

use rk_core::config::ReactorConfig;
use rk_core::id::RecordId;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Pattern, Tuple, FULL_STRENGTH};
use rk_daemon::reactor::{Reactor, REACTOR_INSTANCE};
use rk_daemon::repos::{RepoRecord, RepoRegistry};
use rk_daemon::supervisor::Supervisor;
use rk_daemon::tickets::{NewTicket, Tickets};
use rk_daemon::workflow_exec::{InstanceStatus, Selection, WorkflowEngine};
use rk_daemon::{Client, Daemon};
use serde_json::json;
use sha2::{Digest, Sha256};
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
        merge_mode: Default::default(),
        remote: None,
        host: None,
        activated_policy: None,
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
    build_reactor_and_engine_with_space(layout, config, space).0
}

fn build_reactor_and_engine_with_space(
    layout: &Layout,
    config: ReactorConfig,
    space: rk_space::Space,
) -> (Arc<Reactor>, Arc<WorkflowEngine>) {
    let tickets = Arc::new(Tickets::new(space.clone(), "test-castle".into()));
    let supervisor = Arc::new(
        Supervisor::new(
            layout.clone(),
            "test-castle".into(),
            "fake".into(),
            rk_ledger::Budget::default(),
            rk_ledger::FleetBudget::default(),
            space.clone(),
            tickets.clone(),
        )
        .unwrap(),
    );
    let engine = Arc::new(WorkflowEngine::new(
        layout.clone(),
        supervisor.clone(),
        space.clone(),
        tickets.clone(),
        Default::default(),
        Default::default(),
        "fake".into(),
        false,
        false,
        false,
        Vec::new(),
        vec!["main".into(), "master".into()],
        0,
    ));
    let reactor = Arc::new(Reactor::new(
        space,
        engine.clone(),
        tickets,
        Some(supervisor),
        layout.clone(),
        config,
    ));
    (reactor, engine)
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

/// A minimal open ticket in `scope` for the unblock-dispatch test.
fn new_ticket(title: &str, scope: &str) -> NewTicket {
    NewTicket {
        title: title.into(),
        body: None,
        scope: Some(scope.into()),
        parent: None,
        priority: "normal".into(),
        labels: vec![],
        depends_on: vec![],
        created_by: None,
        coalesce_key: None,
    }
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

fn create_legacy_database(path: &Path, tuples: &[&Tuple]) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE tuples (
            id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            scope TEXT NOT NULL,
            identity TEXT NOT NULL,
            instance TEXT NOT NULL,
            lifecycle TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT,
            strength REAL
        );",
    )
    .unwrap();
    for tuple in tuples {
        conn.execute(
            "INSERT INTO tuples
             (id, category, scope, identity, instance, lifecycle, payload,
              created_at, expires_at, strength)
             VALUES (?1, ?2, ?3, ?4, ?5, 'session', ?6, ?7, NULL, NULL)",
            rusqlite::params![
                tuple.id.to_string(),
                tuple.category.as_str(),
                tuple.scope,
                tuple.identity,
                tuple.instance,
                tuple.payload.to_string(),
                tuple.created_at.to_rfc3339(),
            ],
        )
        .unwrap();
    }
}

fn stable_reactor_instance_id(trigger: &str, tuple: RecordId) -> String {
    let digest = Sha256::digest(format!("{trigger}@{tuple}").as_bytes());
    format!("reactor-{}", hex::encode(&digest[..16]))
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

#[tokio::test]
async fn delayed_lower_record_id_is_processed_after_cursor_advance() {
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
    let now = chrono::Utc::now();
    let mut delayed = ping();
    delayed.id = RecordId::floor_at(now);
    let mut boundary = Tuple::new(
        Category::Event,
        "myrepo",
        "boundary",
        "Whisker",
        json!({}),
    );
    boundary.id = RecordId::floor_at(now + chrono::Duration::days(1));

    space.out(boundary).unwrap();
    assert_eq!(reactor.run_cycle().unwrap(), 0);
    space.out(delayed).unwrap();

    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "persistence order must replay a delayed tuple below the prior ULID cursor"
    );
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn restart_after_future_record_id_processes_normal_write() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout);
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    let now = chrono::Utc::now();
    {
        let space = rk_space::Space::open(&layout.db_path()).unwrap();
        let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());
        let mut future = Tuple::new(
            Category::Event,
            "myrepo",
            "future-boundary",
            "Whisker",
            json!({}),
        );
        future.id = RecordId::floor_at(now + chrono::Duration::days(1));
        space.out(future).unwrap();
        reactor.initialize_cursor().unwrap();
    }

    let reopened = rk_space::Space::open(&layout.db_path()).unwrap();
    let restarted = build_reactor_with_space(
        &layout,
        ReactorConfig::default(),
        reopened.clone(),
    );
    let mut normal = ping();
    normal.id = RecordId::floor_at(now);
    reopened.out(normal).unwrap();

    assert_eq!(
        restarted.run_cycle().unwrap(),
        1,
        "a persisted sequence must survive restart and wall-clock rollback"
    );
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn deleted_tuple_is_still_dispatched_from_the_persistence_journal() {
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
    let tuple = ping();
    let id = tuple.id;
    space.out(tuple).unwrap();
    assert!(space.delete(id).unwrap());

    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "a take or delete before the scan must not erase the persisted insertion event"
    );
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn legacy_ulid_cursor_replays_migrated_history_and_rewrites_decimal() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout);
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    let now = chrono::Utc::now();
    let mut delayed = ping();
    delayed.id = RecordId::floor_at(now);
    let mut old_boundary = Tuple::new(
        Category::Event,
        "myrepo",
        "old-boundary",
        "Whisker",
        json!({}),
    );
    old_boundary.id = RecordId::floor_at(now + chrono::Duration::days(1));
    create_legacy_database(&layout.db_path(), &[&old_boundary, &delayed]);
    std::fs::write(
        home.path().join("reactor-cursor"),
        old_boundary.id.to_string(),
    )
    .unwrap();

    let space = rk_space::Space::open(&layout.db_path()).unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space);
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "legacy conversion must replay a delayed low-ID historical tuple"
    );
    let rewritten = std::fs::read_to_string(home.path().join("reactor-cursor")).unwrap();
    assert_eq!(rewritten.trim().parse::<u64>().unwrap(), 2);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn expired_live_marker_remains_deduplicated_by_the_persistence_journal() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout);
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        marker_ttl_secs: 1,
        ..ReactorConfig::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());
    space.out(ping()).unwrap();
    assert_eq!(reactor.run_cycle().unwrap(), 1);
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    space.gc_expired(0.0).unwrap();
    assert!(space
        .scan(&Pattern::category(Category::Event).identity("reactor_fired"))
        .unwrap()
        .is_empty());
    std::fs::remove_file(home.path().join("reactor-cursor")).unwrap();

    assert_eq!(
        reactor.run_cycle().unwrap(),
        0,
        "expired live markers must remain permanent in the persistence journal"
    );
    assert_eq!(reactor.engine_instance_count(), 1);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn existing_stable_reactor_instance_prevents_duplicate_launch_without_marker() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout);
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    let space = rk_space::Space::open_in_memory().unwrap();
    let (reactor, engine) = build_reactor_and_engine_with_space(
        &layout,
        ReactorConfig::default(),
        space.clone(),
    );
    let tuple = ping();
    let instance_id = stable_reactor_instance_id("ping-drain", tuple.id);
    engine
        .run_owned_with_id(
            instance_id,
            "react-work",
            &repo.path().to_string_lossy(),
            Default::default(),
            None,
        )
        .unwrap();
    assert_eq!(reactor.engine_instance_count(), 1);
    space.out(tuple).unwrap();

    reactor.run_cycle().unwrap();
    assert_eq!(
        reactor.engine_instance_count(),
        1,
        "replaying after a marker-write crash must resolve to the same workflow instance"
    );
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test]
async fn workflow_launch_fails_when_initial_instance_cannot_be_persisted() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    let (_, engine) = build_reactor_and_engine_with_space(
        &layout,
        ReactorConfig::default(),
        space,
    );
    std::fs::write(layout.home().join("workflow-instances"), "not a directory").unwrap();

    let result = engine.run_owned_with_id(
        "must-be-durable".into(),
        "react-work",
        &repo.path().to_string_lossy(),
        Default::default(),
        None,
    );

    assert!(result.is_err(), "a workflow must not launch without durable instance state");
    assert!(engine.status("must-be-durable").is_none());
}

#[tokio::test]
async fn archived_stable_instance_id_prevents_relaunch() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/react-work.cue"),
        r#"workflow: {
            name: "react-work"
            steps: [
                {type: "run", command: "true"},
                {type: "run", command: "true"},
            ]
        }"#,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let (_, engine) = build_reactor_and_engine_with_space(
        &layout,
        ReactorConfig::default(),
        rk_space::Space::open_in_memory().unwrap(),
    );
    let id = "stable-archived".to_string();
    engine
        .run_owned_with_id(
            id.clone(),
            "react-work",
            &repo.path().to_string_lossy(),
            Default::default(),
            None,
        )
        .unwrap();
    for _ in 0..50 {
        if engine
            .status(&id)
            .is_some_and(|instance| instance.status != rk_daemon::workflow_exec::InstanceStatus::Running)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    engine.archive(&Selection::Ids(vec![id.clone()])).unwrap();

    let replay = engine
        .run_owned_with_id(
            id.clone(),
            "react-work",
            &repo.path().to_string_lossy(),
            Default::default(),
            None,
        )
        .unwrap();

    assert!(replay.archived_at.is_some());
    assert!(engine.status(&id).is_none());
    assert_eq!(engine.list_archived().len(), 1);
}

#[test]
fn daemon_rehydrates_workflows_before_starting_dispatch_loops() {
    let source = include_str!("../src/server.rs");
    let rehydrate = source.find("daemon.engine().rehydrate();").unwrap();
    let reactor = source.find("// Reactor loop:").unwrap();
    let scheduler = source.find("// Scheduler loop:").unwrap();

    assert!(rehydrate < reactor);
    assert!(rehydrate < scheduler);
}

/// A matching tuple must remain deliverable when its target repo is briefly
/// unavailable. The old reactor advanced the cursor while logging a warning,
/// permanently dropping the event before an operator could register the repo.
#[tokio::test]
async fn dispatch_retries_after_a_missing_repo_is_registered() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout);

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());
    space.out(ping()).unwrap();

    assert_eq!(
        reactor.run_cycle().unwrap(),
        0,
        "missing repo must be retryable without firing"
    );
    assert!(
        !home.path().join("reactor-cursor").exists(),
        "failed dispatch must not advance the durable cursor"
    );

    register_repo(&layout, "myrepo", repo.path());
    assert_eq!(reactor.run_cycle().unwrap(), 1);
}

/// Dependency-unblock auto-dispatch (TKT-56): closing a ticket emits a
/// `ticket_closed` event, and a reactor trigger matching it hands the newly-ready
/// backlog to a drain workflow. Here TKT-2 depends on TKT-1; closing TKT-1 makes
/// TKT-2 ready AND fires the drain — the DAG advances itself with no operator and
/// no wait for the next drain sweep.
#[tokio::test]
async fn closing_a_ticket_dispatches_its_newly_ready_dependent() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    // A trigger that fires the drain workflow when a ticket closes in myrepo.
    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("unblock.cue"),
        r#"triggers: [{name: "drain-on-unblock", match: {category: "event", scope: "myrepo", identity: "ticket_closed"}, run: "react-work"}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // A ticket and a dependent that is blocked on it, both in myrepo.
    let tickets = Tickets::new(space.clone(), "test-castle".into());
    let a = tickets
        .create(new_ticket("root", "myrepo"))
        .await
        .unwrap();
    let b = tickets
        .create(new_ticket("dependent", "myrepo"))
        .await
        .unwrap();
    tickets.add_dep(&b.identity, &a.identity).await.unwrap();

    // Baseline the cursor past the ticket writes (they are tasks, not events, so
    // nothing fires yet), then confirm the dependent is blocked.
    assert_eq!(reactor.run_cycle().unwrap(), 0, "no close event yet");
    assert!(
        tickets.ready(Some("myrepo".into())).unwrap().iter().all(|t| t.identity != b.identity),
        "dependent is blocked while its dep is open"
    );

    // Close the dep. That emits `ticket_closed`, and the next reactor cycle fires
    // the drain — the dependent is now ready to be picked up.
    tickets.set_status(&a.identity, "done").await.unwrap();
    assert!(
        tickets.ready(Some("myrepo".into())).unwrap().iter().any(|t| t.identity == b.identity),
        "closing the dep unblocked the dependent"
    );
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "ticket_closed fired the drain workflow"
    );
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Triggers are cached across cycles (skipping the per-file `cue` shell-out),
/// but an edited trigger file must still take effect. Rewriting the matcher
/// between cycles changes the file stamp, so the reactor reparses and fires on
/// the new predicate — proving the cache invalidates rather than pinning a
/// stale parse.
#[tokio::test]
async fn edited_trigger_file_is_reparsed_not_stale() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    write_trigger(&layout); // matches identity "ping"
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // First cycle parses the file and fires on the "ping" predicate.
    space.out(ping()).unwrap();
    assert_eq!(reactor.run_cycle().unwrap(), 1, "original trigger fires");

    // Rewrite the same file to match a different identity ("pong-drain" differs
    // in both content and length from the original, so the stamp flips).
    std::fs::write(
        layout.triggers_dir().join("ping.cue"),
        r#"triggers: [{name: "pong-drain-trigger", match: {category: "event", scope: "myrepo", identity: "pong"}, run: "react-work"}]"#,
    )
    .unwrap();

    // A "ping" tuple no longer matches the (reparsed) trigger.
    space.out(ping()).unwrap();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        0,
        "reparsed trigger no longer matches the old predicate"
    );

    // A "pong" tuple matches the edited trigger — the cache picked up the edit.
    let mut pong = ping();
    pong.identity = "pong".into();
    space.out(pong).unwrap();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "edited trigger fires on the new predicate"
    );
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

/// The steward's re-entrancy break (TKT-19): its trigger scopes to
/// `harness_result` completions carrying `"role":"rat"` via the match `search`.
/// A rat completion fires it; a reviewer completion (the very agent the steward
/// spawns) does NOT — so the steward never re-triggers itself on the branch it
/// just reviewed. This pins the `search`-substring scoping the whole design
/// rests on, without needing the reviewer's verdict artifact.
#[tokio::test]
async fn steward_trigger_fires_on_rat_completion_not_reviewer() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    // A minimal steward stand-in: same match predicate the shipped trigger uses
    // (harness_result + `"role":"rat"` payload search), firing the count-only
    // react-work workflow instead of the real steward.
    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("steward.cue"),
        r#"triggers: [{name: "steward-on-completion", match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}, run: "react-work", repo: "myrepo"}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // The reviewer the steward would spawn completes first: role "reviewer".
    // serde_json serializes map keys in insertion order, so the payload renders
    // `"role":"reviewer"` — which the `"role":"rat"` search does NOT contain.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "test-castle",
            json!({"agent": "reviewer-1", "role": "reviewer", "branch": "rat/x/rev"}),
        ))
        .unwrap();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        0,
        "a reviewer completion must NOT fire the steward (re-entrancy break)"
    );

    // A plain rat completion: role "rat" — the search matches, steward fires.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "test-castle",
            json!({"agent": "rat-1", "role": "rat", "branch": "rat/x/work"}),
        ))
        .unwrap();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "a rat completion fires the steward exactly once"
    );
    assert_eq!(reactor.engine_instance_count(), 1);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A rat dispatched from a ticket that is scope "system" (e.g. a sub-ticket
/// minted before parent-scope inheritance landed, or a cross-repo ticket
/// resolved through its parent) still emits its `harness_result` scoped to
/// the real repo it ran in — `route_completion` scopes the event on
/// `record.repo_name`, the resolved filesystem repo, never the ticket's own
/// scope field. So the shipped trigger's `repo: "{{tuple.scope}}"`
/// interpolation must resolve to that real repo, not "system".
#[tokio::test]
async fn steward_trigger_resolves_real_repo_for_system_scoped_ticket_completion() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    // The shipped trigger shape (examples/triggers.cue steward-on-completion):
    // no top-level `repo:` override, so try_fire's fallback chain resolves the
    // target repo straight from the matched tuple's own scope.
    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("steward.cue"),
        r#"triggers: [{name: "steward-on-completion", match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}, run: "react-work", params: {repo: "{{tuple.scope}}"}}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // Simulate the completion of a rat that was dispatched from a
    // system-scoped ticket but actually ran in "myrepo": the event is scoped
    // to the real repo, per route_completion's use of record.repo_name.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "test-castle",
            json!({"agent": "rat-1", "role": "rat", "branch": "rat/x/work"}),
        ))
        .unwrap();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "steward must fire for the rat's real repo even though its ticket was system-scoped"
    );
    assert_eq!(reactor.engine_instance_count(), 1);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A completion tuple carrying a non-`main` `target` (the shipped steward
/// trigger pins `target: "{{tuple.payload.target}}"`, so a rework/chained rat's
/// own `--base` flows straight through) must produce a visible
/// `reactor_non_main_land_target` event — the land target is otherwise
/// indistinguishable from an ordinary run to `main` in `rk workflow list`. A
/// completion whose target IS `main` must not produce that event at all.
#[tokio::test]
async fn non_main_land_target_is_reported_main_is_not() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    // Same shape as the shipped steward trigger: `target` is pinned straight
    // from the completion tuple's payload, not the workflow's own default.
    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("steward.cue"),
        r#"triggers: [{name: "steward-on-completion", match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}, run: "react-work", repo: "myrepo", params: {target: "{{tuple.payload.target}}", branch: "{{tuple.payload.branch}}"}}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let (reactor, engine) =
        build_reactor_and_engine_with_space(&layout, ReactorConfig::default(), space.clone());

    // A rework rat completed chained onto a feature branch: its own `--base`
    // (not "main") is what the completed rat's harness_result carries as
    // `target` (supervisor.rs `record.target_branch`).
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "test-castle",
            json!({
                "agent": "basil-4",
                "role": "rat",
                "branch": "rat/basil-4/rework",
                "target": "rat/camembert-4/tkt-9",
            }),
        ))
        .unwrap();
    assert_eq!(reactor.run_cycle().unwrap(), 1);

    // The launched instance itself must RECORD the inherited target — the
    // visibility event alone is not the recorded steward state. This is what
    // `rk workflow status`/`list` read (list renders " target=<branch>" from
    // exactly this params field; see rk-cli workflow_target_suffix tests).
    let instances = engine.list();
    assert_eq!(instances.len(), 1, "trigger must launch exactly one instance");
    assert_eq!(instances[0].workflow, "react-work");
    assert_eq!(
        instances[0].params.get("target"),
        Some(&json!("rat/camembert-4/tkt-9")),
        "instance must persist the inherited non-main land target"
    );
    assert_eq!(
        instances[0].params.get("branch"),
        Some(&json!("rat/basil-4/rework")),
        "instance must persist the reviewed branch"
    );

    let events = space
        .scan(&Pattern::category(Category::Event).identity("reactor_non_main_land_target"))
        .unwrap();
    assert_eq!(
        events.len(),
        1,
        "a non-main target must produce exactly one visibility event"
    );
    assert_eq!(events[0].scope, "myrepo");
    assert_eq!(events[0].instance, "reactor");
    assert_eq!(events[0].payload["target"], "rat/camembert-4/tkt-9");
    assert_eq!(events[0].payload["branch"], "rat/basil-4/rework");
    assert_eq!(events[0].payload["trigger"], "steward-on-completion");

    // An ordinary completion landing on "main" must NOT produce the event.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "test-castle",
            json!({
                "agent": "basil-5",
                "role": "rat",
                "branch": "rat/basil-5/work",
                "target": "main",
            }),
        ))
        .unwrap();
    assert_eq!(reactor.run_cycle().unwrap(), 1);
    let events = space
        .scan(&Pattern::category(Category::Event).identity("reactor_non_main_land_target"))
        .unwrap();
    assert_eq!(
        events.len(),
        1,
        "a target of \"main\" must not add a second visibility event"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// A trigger param templated from a payload field the matched tuple does not
/// carry (the "legacy completion" case: an older/differently-shaped
/// `harness_result`, or — going forward — any completion predating the
/// `diff_class` enrichment) must still fire, falling back to the target
/// workflow's own declared default rather than hard-failing on a `Null` param.
///
/// Before the null-guard fix, `template_params` passed the absent field
/// through as a present `Value::Null`, which skips the workflow loader's
/// default substitution (`!effective.contains_key(name)`) and fails
/// `coerce_param` for every declared type — so `try_fire` returned `Err`,
/// `run_cycle` swallowed it as a `retryable_failure` (never advancing the
/// cursor), and the trigger would fail on this exact same tuple forever.
#[tokio::test]
async fn trigger_param_over_an_absent_payload_field_falls_back_to_the_workflow_default() {
    const WORKFLOW_WITH_DEFAULTED_PARAM: &str = r#"
        workflow: {
            name: "react-work-tagged"
            agents: {default: {harness: "fake"}}
            params: {
                tag: {type: "string", required: false, default: "no-tag"}
            }
            steps: [
                {type: "spawn", role: "rat", task: {title: "reacted-\(_input.tag)"}},
            ]
        }
    "#;

    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/react-work-tagged.cue"),
        WORKFLOW_WITH_DEFAULTED_PARAM,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);

    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("tagged.cue"),
        r#"triggers: [{name: "tagged-on-completion", match: {category: "event", identity: "harness_result"}, run: "react-work-tagged", repo: "myrepo", params: {tag: "{{tuple.payload.tag}}"}}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // No "tag" field in this payload at all.
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "test-castle",
            json!({"agent": "basil-6", "role": "rat"}),
        ))
        .unwrap();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "a null-templated param must not hard-fail the fire"
    );
    assert_eq!(reactor.engine_instance_count(), 1);

    // The workflow's own default ("no-tag") took effect, not an empty/null
    // value. The instance's `spawn` step runs on a background task, so poll
    // rather than scanning immediately.
    let mut spawns = Vec::new();
    for _ in 0..100 {
        spawns = space
            .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
            .unwrap();
        if !spawns.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(spawns.len(), 1, "the tagged workflow's spawn step never ran");
    assert_eq!(spawns[0].payload["task"], "reacted-no-tag");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// Full path over the wire: suggestion + three distinct endorsers land via
/// `space.out`; the live daemon's reactor loop promotes a convention on its own.
#[tokio::test]
async fn live_daemon_promotes_convention_at_quorum() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let sug = json!({"category": "suggestion", "scope": "system", "identity": "sug-live",
                     "instance": "Whisker", "payload": {"text": "squash before merge"}});
    client.call("space.out", sug).await.unwrap();
    for who in ["Whisker", "Nibbles", "Gouda"] {
        client
            .call(
                "space.out",
                json!({"category": "endorsement", "scope": "system", "identity": "sug-live",
                       "instance": who, "payload": {"suggestion": "sug-live"}}),
            )
            .await
            .unwrap();
    }

    let mut promoted = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let convs = client
            .call("space.scan", json!({"category": "convention", "scope": "system"}))
            .await
            .unwrap();
        if let Some(arr) = convs["tuples"].as_array() {
            if let Some(c) = arr.iter().find(|c| c["identity"] == "sug-live") {
                assert_eq!(c["payload"]["count"], json!(3));
                assert_eq!(c["lifecycle"], json!("furniture"));
                promoted = true;
                break;
            }
        }
    }
    assert!(promoted, "reactor never promoted the quorum-reached suggestion");
}

fn suggestion(id: &str, author: &str, text: &str) -> Tuple {
    Tuple::new(
        Category::Suggestion,
        "system",
        id,
        author,
        json!({"text": text, "agent": author}),
    )
}

fn endorsement(sug_id: &str, endorser: &str) -> Tuple {
    Tuple::new(
        Category::Endorsement,
        "system",
        sug_id,
        endorser,
        json!({"suggestion": sug_id, "agent": endorser}),
    )
}

fn conventions(space: &rk_space::Space) -> Vec<Tuple> {
    space
        .scan(&rk_core::tuple::Pattern::category(Category::Convention))
        .unwrap()
}

/// The flagship stigmergy loop: distinct endorsers reaching quorum promote a
/// suggestion into a permanent (Furniture) system-scope convention exactly once,
/// and a duplicate endorsement from an already-counted agent never inflates the
/// tally.
#[tokio::test]
async fn quorum_promotes_suggestion_to_convention_once() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        quorum: 3,
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    space.out(suggestion("sug-abc", "Whisker", "rebase, never merge")).unwrap();
    space.out(endorsement("sug-abc", "Whisker")).unwrap();
    space.out(endorsement("sug-abc", "Nibbles")).unwrap();
    // A duplicate endorsement from an already-counted agent: must not count.
    space.out(endorsement("sug-abc", "Nibbles")).unwrap();

    // Two distinct endorsers: below quorum, no convention yet.
    reactor.run_cycle().unwrap();
    assert!(conventions(&space).is_empty(), "sub-quorum must not promote");

    // The third distinct endorser trips quorum.
    space.out(endorsement("sug-abc", "Gouda")).unwrap();
    reactor.run_cycle().unwrap();
    let convs = conventions(&space);
    assert_eq!(convs.len(), 1, "quorum promotes exactly one convention");
    let c = &convs[0];
    assert_eq!(c.identity, "sug-abc");
    assert_eq!(c.instance, REACTOR_INSTANCE);
    assert_eq!(c.lifecycle, rk_core::tuple::Lifecycle::Furniture);
    assert_eq!(c.payload["count"], json!(3));
    assert_eq!(c.payload["text"], json!("rebase, never merge"));
    assert_eq!(
        c.payload["endorsers"],
        json!(["Gouda", "Nibbles", "Whisker"]),
        "endorsers are the distinct, sorted set"
    );

    // Idempotent: re-running (even with more endorsements) never double-promotes.
    space.out(endorsement("sug-abc", "Brie")).unwrap();
    reactor.run_cycle().unwrap();
    assert_eq!(conventions(&space).len(), 1, "convention is the promote-once guard");
}

/// A quorum cannot promote a suggestion whose text has already decayed: a
/// permanent Convention without content is junk and must not be minted.
#[tokio::test]
async fn quorum_does_not_promote_after_suggestion_decays() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        quorum: 2,
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    // No Suggestion tuple present (it decayed) — only the endorsements remain.
    space.out(endorsement("sug-gone", "Whisker")).unwrap();
    space.out(endorsement("sug-gone", "Nibbles")).unwrap();
    reactor.run_cycle().unwrap();

    assert!(conventions(&space).is_empty());
}

// --- Obstacle coalescence -------------------------------------------------

/// One raw obstacle trail as the CLI writes it: identity=instance=agent, so a
/// rat holds a single obstacle at a time; `text` is the wall it hit.
fn obstacle(scope: &str, agent: &str, text: &str) -> Tuple {
    let mut t = Tuple::new(
        Category::Obstacle,
        scope,
        agent,
        agent,
        json!({"agent": agent, "text": text}),
    )
    .with_lifecycle(rk_core::tuple::Lifecycle::Ephemeral);
    t.strength = Some(1.0);
    t
}

fn coalesced_tickets(space: &rk_space::Space) -> Vec<Tuple> {
    space
        .scan(&rk_core::tuple::Pattern::category(Category::Task))
        .unwrap()
        .into_iter()
        .filter(|t| t.payload.get("coalesce_key").is_some())
        .collect()
}

/// The reactor files the coalesced ticket via a spawned async create; poll the
/// space until it lands (or give up after a generous budget).
async fn wait_for_coalesced(space: &rk_space::Space, want: usize) -> Vec<Tuple> {
    for _ in 0..100 {
        let t = coalesced_tickets(space);
        if t.len() >= want {
            return t;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    coalesced_tickets(space)
}

/// Ten rats hitting one wall must produce ONE ticket, not ten obstacles with no
/// signal. Sub-quorum stays a gradient; quorum closes the loop to the backlog;
/// re-running never double-files (open ticket + guard marker are the guards).
#[tokio::test]
async fn obstacles_coalesce_into_one_ticket_at_quorum() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        coalesce_quorum: 3,
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    // Two distinct rats hit the same wall; one of them restates it (a second
    // tuple on the same instance). Distinct reporters = 2, below quorum.
    space
        .out(obstacle("myrepo", "Whisker", "cargo build fails on rk-space"))
        .unwrap();
    space
        .out(obstacle("myrepo", "Whisker", "cargo build FAILS on rk-space!!"))
        .unwrap();
    space
        .out(obstacle("myrepo", "Nibbles", "Cargo build fails on rk-space."))
        .unwrap();
    reactor.run_cycle().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        coalesced_tickets(&space).is_empty(),
        "two distinct reporters is sub-quorum — no ticket, restating does not inflate"
    );
    // Coalescence never injects synthetic obstacles into the raw pile.
    let reactor_obstacles = space
        .scan(&rk_core::tuple::Pattern::category(Category::Obstacle))
        .unwrap()
        .into_iter()
        .filter(|t| t.instance == REACTOR_INSTANCE)
        .count();
    assert_eq!(reactor_obstacles, 0, "the obstacle pile stays rat-authored");

    // A third distinct rat trips quorum → exactly one ticket is filed.
    space
        .out(obstacle("myrepo", "Sooty", "cargo build fails on rk-space"))
        .unwrap();
    reactor.run_cycle().unwrap();
    let tickets = wait_for_coalesced(&space, 1).await;
    assert_eq!(tickets.len(), 1, "quorum files exactly one ticket");
    let t = &tickets[0];
    assert_eq!(t.scope, "myrepo");
    assert_eq!(t.payload["status"], "open");
    assert_eq!(t.instance, "test-castle");
    assert_eq!(t.payload["created_by"], json!(REACTOR_INSTANCE));
    assert_eq!(t.payload["labels"], json!(["obstacle-coalesce"]));
    assert!(
        t.payload["coalesce_key"]
            .as_str()
            .is_some_and(|k| k.starts_with("myrepo::")),
        "ticket carries the scope-qualified dedupe key"
    );

    // Idempotent: further cycles (even with the wall still hot) never re-file —
    // the open ticket and the guard marker suppress it.
    reactor.run_cycle().unwrap();
    reactor.run_cycle().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        coalesced_tickets(&space).len(),
        1,
        "an open coalesced ticket files once until closed"
    );
}

/// A different wall in a different repo is a different topic → its own ticket;
/// coalescence is per (scope, normalised topic), never a global merge.
#[tokio::test]
async fn distinct_topics_and_scopes_file_separate_tickets() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        coalesce_quorum: 2,
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    // Wall A in repo one.
    space.out(obstacle("one", "Whisker", "flaky network test")).unwrap();
    space.out(obstacle("one", "Nibbles", "flaky network test")).unwrap();
    // Same words, different repo → different topic key.
    space.out(obstacle("two", "Whisker", "flaky network test")).unwrap();
    space.out(obstacle("two", "Gouda", "flaky network test")).unwrap();
    // A wholly different wall in repo one, at quorum too.
    space.out(obstacle("one", "Brie", "missing config key")).unwrap();
    space.out(obstacle("one", "Sooty", "missing config key")).unwrap();

    reactor.run_cycle().unwrap();
    let tickets = wait_for_coalesced(&space, 3).await;
    let mut keys: Vec<String> = tickets
        .iter()
        .map(|t| t.payload["coalesce_key"].as_str().unwrap().to_string())
        .collect();
    keys.sort();
    assert_eq!(keys.len(), 3, "three distinct (scope, topic) buckets → three tickets");
    assert!(keys.iter().any(|k| k.starts_with("one::") && k.contains("flaky")));
    assert!(keys.iter().any(|k| k.starts_with("two::") && k.contains("flaky")));
    assert!(keys.iter().any(|k| k.starts_with("one::") && k.contains("missing")));
}

/// Coalescence is off when the quorum is zero: the pile stays flat.
#[tokio::test]
async fn zero_quorum_disables_coalescence() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        coalesce_quorum: 0,
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    for agent in ["a", "b", "c", "d"] {
        space.out(obstacle("myrepo", agent, "the same wall")).unwrap();
    }
    reactor.run_cycle().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(coalesced_tickets(&space).is_empty(), "zero quorum files nothing");
}

/// A steward escalation `need`: identity "steward", as `rk out need <repo>
/// steward` writes it (instance = castle, not the escalating agent).
fn steward_need(scope: &str, task: &str, text: &str) -> Tuple {
    let mut t = Tuple::new(
        Category::Need,
        scope,
        "steward",
        "test-castle",
        json!({"agent": "steward", "task": task, "text": text}),
    )
    .with_lifecycle(rk_core::tuple::Lifecycle::Ephemeral);
    t.strength = Some(1.0);
    t
}

/// Count the durable "already notified" markers the escalation push writes.
fn escalation_markers(space: &rk_space::Space) -> usize {
    space
        .scan(&rk_core::tuple::Pattern::category(Category::Event).identity("reactor_fired"))
        .unwrap()
        .into_iter()
        .filter(|t| t.payload.get("kind").and_then(|k| k.as_str()) == Some("notify-escalation"))
        .count()
}

/// A steward escalation gets an active push exactly once, and only the steward:
/// an ordinary rat's `rk need` (identity = agent name) is left on the passive
/// inbox queue. The push degrades to a no-op with no herdr server, so we assert
/// on the durable de-dup marker it leaves behind rather than the popup itself.
#[tokio::test]
async fn steward_escalation_notifies_once_ordinary_need_does_not() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // A steward STOP escalation and an ordinary rat help request land together.
    space
        .out(steward_need(
            "myrepo",
            "TKT-99",
            "steward: reviewer returned STOP for TKT-99 — needs a human merge decision",
        ))
        .unwrap();
    let mut rat_need = steward_need("myrepo", "TKT-1", "cannot find the socket");
    rat_need.identity = "Whisker".into(); // an ordinary rat need keys on its agent
    space.out(rat_need).unwrap();

    reactor.run_cycle().unwrap();
    assert_eq!(
        escalation_markers(&space),
        1,
        "only the steward escalation is pushed; a rat's own need is not"
    );

    // Idempotent under at-least-once redelivery: wipe the cursor so both needs
    // are re-scanned — the durable marker still suppresses a second push.
    std::fs::remove_file(home.path().join("reactor-cursor")).ok();
    reactor.run_cycle().unwrap();
    assert_eq!(
        escalation_markers(&space),
        1,
        "the marker prevents a duplicate popup after cursor loss"
    );
}

/// `notify_escalations = false` keeps escalations purely on the `rk inbox`
/// queue: no active push, no marker.
#[tokio::test]
async fn escalation_notify_can_be_disabled() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        notify_escalations: false,
        ..Default::default()
    };
    let reactor = build_reactor_with_space(&layout, config, space.clone());

    space
        .out(steward_need("myrepo", "TKT-7", "steward: STOP, needs a human"))
        .unwrap();
    reactor.run_cycle().unwrap();
    assert_eq!(
        escalation_markers(&space),
        0,
        "the push is off, so no notification marker is written"
    );
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

// --- Resolution backlinks (TKT-28) ----------------------------------------

/// An artifact that resolves a wall, as `rk out artifact ... --resolves <id>`
/// writes it: the resolved tuple id rides in `payload.resolves`.
fn resolving_artifact(name: &str, resolves_id: &str) -> Tuple {
    Tuple::new(
        Category::Artifact,
        "myrepo",
        name,
        "Whisker",
        json!({"note": "fixed it", "resolves": resolves_id}),
    )
}

fn resolutions(space: &rk_space::Space) -> Vec<Tuple> {
    space
        .scan(&Pattern::category(Category::Resolution))
        .unwrap()
}

/// A resolving artifact retires the exact wall it names and lays a decaying
/// (topic -> artifact) trail, at full strength, authored by the reactor.
#[tokio::test]
async fn resolving_artifact_retires_wall_and_lays_resolution_trail() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // A rat files a wall; an artifact then resolves it.
    let wall = obstacle("myrepo", "Whisker", "cargo build fails on rk-space");
    space.out(wall.clone()).unwrap();
    let art = resolving_artifact("build-fix", &wall.id.to_string());
    space.out(art.clone()).unwrap();

    reactor.run_cycle().unwrap();

    // The solved wall is retired.
    let obstacles = space.scan(&Pattern::category(Category::Obstacle)).unwrap();
    assert!(
        obstacles.iter().all(|t| t.id != wall.id),
        "resolved wall is retired"
    );

    // A single (topic -> artifact) trail was laid, keyed on the normalised topic.
    let trails = resolutions(&space);
    assert_eq!(trails.len(), 1, "one resolution trail laid");
    let trail = &trails[0];
    assert_eq!(
        trail.identity, "cargo build fails on rk space",
        "trail is keyed on the normalised topic"
    );
    assert_eq!(trail.payload["artifact_id"], json!(art.id.to_string()));
    assert_eq!(trail.payload["resolved"], json!(wall.id.to_string()));
    assert_eq!(trail.instance, REACTOR_INSTANCE);
    assert_eq!(
        trail.strength,
        Some(FULL_STRENGTH),
        "laid at full strength; a trail nobody re-needs decays via GC (TKT-14)"
    );
}

/// A fresh obstacle on an already-resolved topic steers the reporting rat to the
/// prior fix (a directed message) and reinforces the trail back to full strength
/// — and the steer is emitted once per obstacle even under cursor-reset replay.
#[tokio::test]
async fn fresh_obstacle_on_resolved_topic_steers_and_reinforces() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());

    // Lay a trail by resolving a first wall.
    let wall1 = obstacle("myrepo", "Whisker", "flaky sync test");
    space.out(wall1.clone()).unwrap();
    let art = resolving_artifact("sync-fix", &wall1.id.to_string());
    space.out(art.clone()).unwrap();
    reactor.run_cycle().unwrap();
    let trail0 = resolutions(&space).pop().expect("trail laid");

    // Decay it so reinforcement is observable.
    space.gc_expired(0.5).unwrap();
    let decayed = resolutions(&space).pop().expect("trail survives one decay");
    assert!(decayed.strength.unwrap() < FULL_STRENGTH, "trail decayed");

    // Another rat hits the same wall (different phrasing, same topic).
    let wall2 = obstacle("myrepo", "Nibbles", "Flaky SYNC test!!");
    space.out(wall2.clone()).unwrap();
    reactor.run_cycle().unwrap();

    // The rat is steered: a directed resolution_steer message pointing at the fix.
    let steer = space
        .scan(
            &Pattern::category(Category::Message)
                .scope("myrepo")
                .identity("Nibbles"),
        )
        .unwrap()
        .into_iter()
        .find(|m| m.payload["type"] == json!("resolution_steer"))
        .expect("a steer message for the reporting rat");
    assert_eq!(steer.payload["artifact_id"], json!(art.id.to_string()));
    assert_eq!(steer.payload["obstacle"], json!(wall2.id.to_string()));
    assert_eq!(steer.instance, REACTOR_INSTANCE);

    // The trail is reinforced in place, back to full strength.
    let reinforced = resolutions(&space).pop().expect("trail still present");
    assert_eq!(reinforced.id, trail0.id, "same trail, refreshed in place");
    assert_eq!(
        reinforced.strength,
        Some(FULL_STRENGTH),
        "a still-live wall reinforces its resolution trail"
    );

    // Idempotent: replaying the same obstacle after a cursor reset emits no
    // second steer.
    std::fs::remove_file(home.path().join("reactor-cursor")).ok();
    reactor.run_cycle().unwrap();
    let steers = space
        .scan(
            &Pattern::category(Category::Message)
                .scope("myrepo")
                .identity("Nibbles"),
        )
        .unwrap()
        .into_iter()
        .filter(|m| m.payload["type"] == json!("resolution_steer"))
        .count();
    assert_eq!(steers, 1, "one steer per obstacle, even after cursor reset");
}

// --- Admission control (maxInFlight) --------------------------------------

/// A burst of 5 completions against a `maxInFlight: 2` trigger dispatches
/// exactly 2 immediately and durably queues the rest — never drops them —
/// draining the queue automatically as running instances complete.
#[tokio::test]
async fn max_in_flight_queues_a_burst_and_drains_it_as_slots_free() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/slow-work.cue"),
        r#"workflow: { name: "slow-work", steps: [{type: "run", command: "sleep 1"}] }"#,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());

    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("burst.cue"),
        r#"triggers: [{name: "burst-drain", match: {category: "event", scope: "myrepo", identity: "burst"}, run: "slow-work", maxInFlight: 2}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let (reactor, engine) =
        build_reactor_and_engine_with_space(&layout, ReactorConfig::default(), space.clone());

    for i in 0..5 {
        space
            .out(Tuple::new(
                Category::Event,
                "myrepo",
                "burst",
                "Whisker",
                json!({"i": i}),
            ))
            .unwrap();
    }
    assert_eq!(
        reactor.run_cycle().unwrap(),
        2,
        "only 2 dispatch immediately at the cap"
    );
    assert_eq!(
        engine
            .list()
            .iter()
            .filter(|i| i.status == InstanceStatus::Running)
            .count(),
        2,
        "exactly 2 instances running"
    );
    let queued = space
        .scan(&Pattern::category(Category::Event).identity("reactor_queued_fire"))
        .unwrap();
    assert_eq!(queued.len(), 3, "the remaining 3 are durably queued, not dropped");

    // Drive the reactor by hand (simulated feed loss) as slots free.
    let mut ran = 0;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        reactor.run_cycle().unwrap();
        ran = engine.list().len();
        let queue_now = space
            .scan(&Pattern::category(Category::Event).identity("reactor_queued_fire"))
            .unwrap()
            .len();
        if ran == 5 && queue_now == 0 {
            break;
        }
    }
    assert_eq!(ran, 5, "all 5 eventually ran");
    assert!(
        space
            .scan(&Pattern::category(Category::Event).identity("reactor_queued_fire"))
            .unwrap()
            .is_empty(),
        "the queue drains to empty"
    );
}

/// `drain_queued_fires` must consult the SAME rolling rate cap an immediate
/// fire does (rework 2 of TKT-01M036NWE1EW5B1PWSHK0MKX8E): a burst held
/// behind `maxInFlight` must not drain past `maxFires` in one window the
/// moment slots start freeing. `maxFires: 2` alongside `maxInFlight: 1` means
/// exactly 2 of the 5 queued tuples ever fire — one immediately, one drained
/// — and the remaining 3 stay queued for good within this test's wide window.
#[tokio::test]
async fn drain_queued_fires_respects_the_rate_cap_not_just_max_in_flight() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/slow-work.cue"),
        r#"workflow: { name: "slow-work", steps: [{type: "run", command: "sleep 1"}] }"#,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());

    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("burst.cue"),
        r#"triggers: [{name: "burst-drain", match: {category: "event", scope: "myrepo", identity: "burst"}, run: "slow-work", maxInFlight: 1, maxFires: 2}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let config = ReactorConfig {
        window_secs: 3600, // one wide window for the whole test
        ..Default::default()
    };
    let (reactor, engine) = build_reactor_and_engine_with_space(&layout, config, space.clone());

    for i in 0..5 {
        space
            .out(Tuple::new(
                Category::Event,
                "myrepo",
                "burst",
                "Whisker",
                json!({"i": i}),
            ))
            .unwrap();
    }
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "only 1 dispatches immediately at the maxInFlight cap of 1"
    );

    // Drive cycles by hand well past the point both fires would complete
    // (2 * 1s sleep) plus headroom, and past the point 5 would complete if
    // the rate cap were not honoured while draining.
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        reactor.run_cycle().unwrap();
        if engine.list().len() >= 3 {
            break; // would only happen if the rate cap failed to hold
        }
    }

    assert_eq!(
        engine.list().len(),
        2,
        "maxFires of 2 must bound total dispatches even though maxInFlight kept freeing slots"
    );
    let queued = space
        .scan(&Pattern::category(Category::Event).identity("reactor_queued_fire"))
        .unwrap();
    assert_eq!(
        queued.len(),
        3,
        "the remaining 3 stay queued once the rate cap is hit, not silently dropped"
    );
}

/// A `RecordId`'s same-millisecond suffix is random, so sorting the queue by
/// `t.id` (the pre-fix behaviour) does not reliably preserve enqueue order.
/// This enqueues enough tuples in rapid succession that several land in the
/// same millisecond in an in-memory space, then asserts `drain_queued_fires`
/// dispatches them in the exact order they were queued — which only the
/// durable `seq` field (rework 2 of TKT-01M036NWE1EW5B1PWSHK0MKX8E) can
/// guarantee.
#[tokio::test]
async fn queued_fires_dispatch_in_enqueue_order_not_record_id_order() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/fifo-work.cue"),
        r#"workflow: { name: "fifo-work", steps: [{type: "run", command: "sleep 0.05"}] }"#,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());

    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("fifo.cue"),
        r#"triggers: [{name: "fifo-drain", match: {category: "event", scope: "myrepo", identity: "fifo"}, run: "fifo-work", maxInFlight: 1}]"#,
    )
    .unwrap();

    const N: usize = 25;
    let space = rk_space::Space::open_in_memory().unwrap();
    // This test is about ORDER, not the rate cap — raise `max_fires` above N
    // so the default cap of 20 (ReactorConfig::default) cannot itself stall
    // the drain before all N have had a chance to dispatch.
    let config = ReactorConfig {
        window_secs: 3600,
        max_fires: N as u32 + 10,
        ..Default::default()
    };
    let (reactor, engine) = build_reactor_and_engine_with_space(&layout, config, space.clone());

    let mut expected_ids = Vec::with_capacity(N);
    for i in 0..N {
        let t = Tuple::new(
            Category::Event,
            "myrepo",
            "fifo",
            "Whisker",
            json!({"i": i}),
        );
        expected_ids.push(stable_reactor_instance_id("fifo-drain", t.id));
        space.out(t).unwrap();
    }

    // maxInFlight of 1 forces strict serialization: the just-launched instance
    // has not finished by the time drain_queued_fires rechecks the live count
    // within the same synchronous call, so at most one new dispatch appears
    // per `run_cycle`.
    let mut dispatch_order = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        reactor.run_cycle().unwrap();
        for inst in engine.list() {
            if seen.insert(inst.id.clone()) {
                dispatch_order.push(inst.id.clone());
            }
        }
        if dispatch_order.len() == N {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        dispatch_order.len(),
        N,
        "all queued fires must eventually dispatch"
    );
    assert_eq!(
        dispatch_order, expected_ids,
        "queued fires must dispatch in enqueue order, not RecordId order"
    );
}

/// A permanently-failing fire (here: a workflow's missing required param) must
/// not pin the reactor's global cursor forever. After `MAX_FIRE_ATTEMPTS`
/// cycles the reactor gives up on that `(trigger, tuple)` for good, and the
/// cursor advances past it instead of re-scanning a growing backlog forever.
#[tokio::test]
async fn permanently_failing_fire_gives_up_and_unpins_the_cursor() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/needs-param.cue"),
        r#"workflow: {
            name: "needs-param"
            params: { must: {type: "string", required: true} }
            steps: [{type: "run", command: "true"}]
        }"#,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());

    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("needy.cue"),
        r#"triggers: [{name: "needy-drain", match: {category: "event", scope: "myrepo", identity: "needy"}, run: "needs-param", params: {must: "{{tuple.payload.absent}}"}}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let reactor = build_reactor_with_space(&layout, ReactorConfig::default(), space.clone());
    space
        .out(Tuple::new(
            Category::Event,
            "myrepo",
            "needy",
            "Whisker",
            json!({}),
        ))
        .unwrap();

    // Bounded retries: the cursor stays pinned while attempts are exhausted.
    for attempt in 1..5 {
        assert_eq!(reactor.run_cycle().unwrap(), 0, "attempt {attempt} must not fire");
        assert!(
            !home.path().join("reactor-cursor").exists(),
            "attempt {attempt}: still retrying, cursor must stay pinned"
        );
    }
    // The final attempt gives up: no fire, but the cursor now advances.
    assert_eq!(reactor.run_cycle().unwrap(), 0);
    assert!(
        home.path().join("reactor-cursor").exists(),
        "after MAX_FIRE_ATTEMPTS the reactor must give up and unpin the cursor"
    );
    let gave_up = space
        .scan(&Pattern::category(Category::Obstacle).identity("reactor_fire_gave_up"))
        .unwrap();
    assert_eq!(gave_up.len(), 1);

    // A later, unrelated tuple on a healthy trigger still dispatches normally.
    write_trigger(&layout);
    std::env::set_var("RK_FAKE_HARNESS_CMD", WORKING_FAKE);
    space.out(ping()).unwrap();
    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "a later unrelated trigger must still dispatch after the give-up"
    );
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// The durable queue behind a trigger's `maxInFlight` cap survives a daemon
/// restart: reopening the space and rebuilding the reactor/engine from the
/// same durable layout still sees the queued fire and dispatches it once its
/// slot is free.
#[tokio::test]
async fn queued_fire_survives_daemon_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/quick-work.cue"),
        r#"workflow: { name: "quick-work", steps: [{type: "run", command: "true"}] }"#,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());

    let dir = layout.triggers_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("burst.cue"),
        r#"triggers: [{name: "burst-drain", match: {category: "event", scope: "myrepo", identity: "burst"}, run: "quick-work", maxInFlight: 1}]"#,
    )
    .unwrap();

    {
        let space = rk_space::Space::open(&layout.db_path()).unwrap();
        let (reactor, engine) =
            build_reactor_and_engine_with_space(&layout, ReactorConfig::default(), space.clone());
        space
            .out(Tuple::new(Category::Event, "myrepo", "burst", "Whisker", json!({"i": 0})))
            .unwrap();
        space
            .out(Tuple::new(Category::Event, "myrepo", "burst", "Whisker", json!({"i": 1})))
            .unwrap();
        assert_eq!(reactor.run_cycle().unwrap(), 1, "only one dispatches at the cap of 1");
        // Wait for the single dispatched instance to settle before "restart",
        // so no background execution task outlives this scope.
        for _ in 0..100 {
            if engine
                .list()
                .iter()
                .all(|i| i.status != InstanceStatus::Running)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(engine.list().len(), 1);
        let queued = space
            .scan(&Pattern::category(Category::Event).identity("reactor_queued_fire"))
            .unwrap();
        assert_eq!(queued.len(), 1, "the second is durably queued before restart");
    }

    // "Restart": reopen the space and rebuild the reactor/engine from the same
    // durable layout, exactly as the daemon does on boot.
    let reopened = rk_space::Space::open(&layout.db_path()).unwrap();
    let (reactor, engine) =
        build_reactor_and_engine_with_space(&layout, ReactorConfig::default(), reopened.clone());
    engine.rehydrate();
    assert_eq!(
        reopened
            .scan(&Pattern::category(Category::Event).identity("reactor_queued_fire"))
            .unwrap()
            .len(),
        1,
        "the queue entry survived the restart"
    );
    assert_eq!(engine.list().len(), 1, "the completed instance rehydrated too");

    assert_eq!(
        reactor.run_cycle().unwrap(),
        1,
        "the queued fire dispatches once rehydrated state shows a free slot"
    );
    assert_eq!(engine.list().len(), 2);
    assert!(
        reopened
            .scan(&Pattern::category(Category::Event).identity("reactor_queued_fire"))
            .unwrap()
            .is_empty(),
        "the queue is empty after the restart-drained dispatch"
    );
}
