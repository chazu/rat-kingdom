//! Scheduler end-to-end + guard tests. The `Scheduler` is driven by hand via
//! `run_cycle_at`, which injects "now" so cron cadence is exercised
//! deterministically without waiting on the wall clock. They pin down: a
//! matching cron minute fires the workflow, the durable minute-cursor makes
//! catch-up fire a missed schedule exactly once, per-schedule single-flight
//! suppresses a stacked run, and an unresolvable repo / malformed cron is
//! skipped rather than fatal.

use chrono::{TimeZone, Utc};
use rk_core::config::SchedulerConfig;
use rk_core::paths::Layout;
use rk_daemon::repos::{RepoRecord, RepoRegistry};
use rk_daemon::scheduler::Scheduler;
use rk_daemon::supervisor::Supervisor;
use rk_daemon::tickets::Tickets;
use rk_daemon::workflow_exec::WorkflowEngine;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

// A one-step workflow the scheduler fires: a long timer gate. It needs no agent
// or harness, and — crucially — its instance stays `Running` for the gate's
// whole duration, which is what the single-flight guard keys on.
const GATE_WORKFLOW: &str = r#"
workflow: {
    name: "sched-work"
    steps: [
        {type: "gate", gateType: "timer", duration: "30s"},
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
    std::fs::write(wf_dir.join("sched-work.cue"), GATE_WORKFLOW).unwrap();
}

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

/// Write a global schedule file (`<home>/schedules/*.cue`).
fn write_global_schedule(layout: &Layout, file: &str, body: &str) {
    let dir = layout.schedules_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(file), body).unwrap();
}

fn build_engine(layout: &Layout, space: rk_space::Space) -> Arc<WorkflowEngine> {
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
    Arc::new(WorkflowEngine::new(
        layout.clone(),
        supervisor,
        space,
        tickets,
        Default::default(),
        Default::default(),
        "fake".into(),
        false,
        false,
        false,
        Vec::new(),
        vec!["main".into(), "master".into()],
    ))
}

fn build_scheduler(layout: &Layout, config: SchedulerConfig, space: rk_space::Space) -> Arc<Scheduler> {
    let engine = build_engine(layout, space);
    Arc::new(Scheduler::new(engine, layout.clone(), config))
}

/// A matching cron minute fires the workflow exactly once; the cursor then
/// suppresses a re-fire within the same minute.
#[tokio::test]
async fn matching_minute_fires_once_and_cursor_suppresses_repeat() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "tick.cue",
        r#"schedules: [{name: "every-min", cron: "* * * * *", run: "sched-work", repo: "myrepo"}]"#,
    );

    let space = rk_space::Space::open_in_memory().unwrap();
    let scheduler = build_scheduler(&layout, SchedulerConfig::default(), space);

    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    // First cycle at minute :00 -> fires (no cursor yet, evaluates this minute).
    assert_eq!(scheduler.run_cycle_at(t).unwrap(), 1, "cron minute fires");
    // Same minute again -> cursor blocks it.
    assert_eq!(
        scheduler.run_cycle_at(t).unwrap(),
        0,
        "cursor suppresses re-fire in the same minute"
    );
    assert_eq!(scheduler.engine_instance_count(), 1);
}

/// First boot baselines the cursor to now, so nothing fires for the current
/// minute; the next minute fires normally.
#[tokio::test]
async fn first_boot_baselines_cursor_and_skips_current_minute() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "tick.cue",
        r#"schedules: [{name: "every-min", cron: "* * * * *", run: "sched-work", repo: "myrepo"}]"#,
    );

    let space = rk_space::Space::open_in_memory().unwrap();
    let scheduler = build_scheduler(&layout, SchedulerConfig::default(), space);

    // initialize_cursor writes the real current minute; drive the first cycle at
    // that same minute so the baseline suppresses it.
    scheduler.initialize_cursor().unwrap();
    let now = Utc::now();
    assert_eq!(
        scheduler.run_cycle_at(now).unwrap(),
        0,
        "baselined minute does not fire"
    );
    // A minute later, it fires.
    let next = now + chrono::Duration::minutes(1);
    assert_eq!(scheduler.run_cycle_at(next).unwrap(), 1, "next minute fires");
    assert_eq!(scheduler.engine_instance_count(), 1);
}

/// After downtime the scheduler catches up: a schedule with several matching
/// minutes in the gap fires exactly once, not once per missed minute.
#[tokio::test]
async fn catchup_fires_missed_schedule_once() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "tick.cue",
        r#"schedules: [{name: "every-5", cron: "*/5 * * * *", run: "sched-work", repo: "myrepo"}]"#,
    );

    let space = rk_space::Space::open_in_memory().unwrap();
    let scheduler = build_scheduler(&layout, SchedulerConfig::default(), space);

    // Seed the cursor at 08:00 (simulating the daemon last ran then), then wake
    // at 08:10 — minutes :05 and :10 both match */5, but only one fire happens.
    let t0 = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    std::fs::write(home.path().join("scheduler-cursor"), t0.to_rfc3339()).unwrap();
    let woke = Utc.with_ymd_and_hms(2026, 7, 23, 8, 10, 0).unwrap();
    assert_eq!(
        scheduler.run_cycle_at(woke).unwrap(),
        1,
        "catch-up fires the missed schedule exactly once"
    );
    assert_eq!(scheduler.engine_instance_count(), 1);
}

/// Per-schedule single-flight: while a schedule's prior run is still `Running`
/// (its gate has not elapsed), a later matching minute does NOT stack a second
/// run.
#[tokio::test]
async fn single_flight_skips_while_previous_run_active() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "tick.cue",
        r#"schedules: [{name: "every-min", cron: "* * * * *", run: "sched-work", repo: "myrepo"}]"#,
    );

    let space = rk_space::Space::open_in_memory().unwrap();
    let scheduler = build_scheduler(&layout, SchedulerConfig::default(), space);

    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    // First minute fires; the 30s timer gate keeps that instance Running.
    assert_eq!(scheduler.run_cycle_at(t).unwrap(), 1, "first minute fires");
    // Next minute: previous run still active -> single-flight skips.
    let t1 = t + chrono::Duration::minutes(1);
    assert_eq!(
        scheduler.run_cycle_at(t1).unwrap(),
        0,
        "single-flight suppresses the stacked run"
    );
    assert_eq!(scheduler.engine_instance_count(), 1, "no second instance");
}

#[tokio::test]
async fn restart_rebuilds_single_flight_from_durable_running_work() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "tick.cue",
        r#"schedules: [{name: "every-min", cron: "* * * * *", run: "sched-work", repo: "myrepo"}]"#,
    );

    let first = build_scheduler(
        &layout,
        SchedulerConfig::default(),
        rk_space::Space::open_in_memory().unwrap(),
    );
    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    assert_eq!(first.run_cycle_at(t).unwrap(), 1);

    let restarted_engine = build_engine(&layout, rk_space::Space::open_in_memory().unwrap());
    let resumable = restarted_engine.rehydrate();
    assert_eq!(resumable.len(), 1, "the in-flight schedule must rehydrate");
    let restarted = Scheduler::new(
        restarted_engine,
        layout.clone(),
        SchedulerConfig::default(),
    );

    assert_eq!(
        restarted.run_cycle_at(t + chrono::Duration::minutes(1)).unwrap(),
        0,
        "a restart must not stack the same scheduled work while it is still running",
    );
    assert_eq!(restarted.engine_instance_count(), 1);
}

#[tokio::test]
async fn restart_matches_legacy_schedule_by_definition_alias() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    std::fs::write(
        repo.path().join(".rk/workflows/nightly-alias.cue"),
        GATE_WORKFLOW,
    )
    .unwrap();
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "tick.cue",
        r#"schedules: [{name: "every-min", cron: "* * * * *", run: "nightly-alias", repo: "myrepo"}]"#,
    );

    let first = build_scheduler(
        &layout,
        SchedulerConfig::default(),
        rk_space::Space::open_in_memory().unwrap(),
    );
    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    assert_eq!(first.run_cycle_at(t).unwrap(), 1);

    let snapshots = layout.home().join("workflow-instances");
    let snapshot = std::fs::read_dir(&snapshots)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut legacy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&snapshot).unwrap()).unwrap();
    legacy.as_object_mut().unwrap().remove("schedule");
    std::fs::write(&snapshot, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();

    let restarted_engine = build_engine(&layout, rk_space::Space::open_in_memory().unwrap());
    assert_eq!(restarted_engine.rehydrate().len(), 1);
    let restarted = Scheduler::new(
        restarted_engine,
        layout.clone(),
        SchedulerConfig::default(),
    );

    assert_eq!(
        restarted.run_cycle_at(t + chrono::Duration::minutes(1)).unwrap(),
        0,
        "legacy snapshots must match the invoked definition alias, not the workflow's internal name",
    );
    assert_eq!(restarted.engine_instance_count(), 1);
}

#[tokio::test]
async fn restart_does_not_use_a_linked_nested_child_as_schedule_single_flight() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "tick.cue",
        r#"schedules: [{name: "every-min", cron: "* * * * *", run: "sched-work", repo: "myrepo"}]"#,
    );

    let first = build_scheduler(
        &layout,
        SchedulerConfig::default(),
        rk_space::Space::open_in_memory().unwrap(),
    );
    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    assert_eq!(first.run_cycle_at(t).unwrap(), 1);

    let snapshots = layout.home().join("workflow-instances");
    let child_path = std::fs::read_dir(&snapshots)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut child: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&child_path).unwrap()).unwrap();
    let child_id = child["id"].as_str().unwrap().to_string();
    child.as_object_mut().unwrap().remove("schedule");
    child["depth"] = serde_json::json!(1);
    std::fs::write(&child_path, serde_json::to_vec_pretty(&child).unwrap()).unwrap();

    let mut parent = child.clone();
    parent["id"] = serde_json::json!("legacy-parent");
    parent["workflow"] = serde_json::json!("parent-workflow");
    parent["definition"] = serde_json::json!("parent-workflow");
    parent["definition_digest"] = serde_json::json!("");
    parent["depth"] = serde_json::json!(0);
    parent["context"]["active_subworkflow"] = serde_json::json!(child_id);
    std::fs::write(
        snapshots.join("legacy-parent.json"),
        serde_json::to_vec_pretty(&parent).unwrap(),
    )
    .unwrap();

    let restarted_engine = build_engine(&layout, rk_space::Space::open_in_memory().unwrap());
    assert_eq!(restarted_engine.rehydrate().len(), 1);
    let restarted = Scheduler::new(
        restarted_engine,
        layout.clone(),
        SchedulerConfig::default(),
    );

    assert_eq!(
        restarted.run_cycle_at(t + chrono::Duration::minutes(1)).unwrap(),
        1,
        "a nested child belongs to its parent, not the top-level schedule guard",
    );
    assert_eq!(restarted.engine_instance_count(), 3);
}

#[tokio::test]
async fn distinct_schedules_may_run_the_same_work_concurrently() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "twins.cue",
        r#"schedules: [
            {name: "first", cron: "* * * * *", run: "sched-work", repo: "myrepo"},
            {name: "second", cron: "* * * * *", run: "sched-work", repo: "myrepo"},
        ]"#,
    );

    let scheduler = build_scheduler(
        &layout,
        SchedulerConfig::default(),
        rk_space::Space::open_in_memory().unwrap(),
    );
    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();

    assert_eq!(
        scheduler.run_cycle_at(t).unwrap(),
        2,
        "single-flight is per schedule name, not per workflow work key"
    );
    assert_eq!(scheduler.engine_instance_count(), 2);
}

/// A global schedule with no `repo` cannot resolve (no tuple scope to fall back
/// on) and is skipped; a malformed cron is likewise skipped, not fatal.
#[tokio::test]
async fn unresolvable_repo_and_bad_cron_are_skipped() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    write_global_schedule(
        &layout,
        "bad.cue",
        r#"schedules: [
            {name: "no-repo", cron: "* * * * *", run: "sched-work"},
            {name: "bad-cron", cron: "not a cron", run: "sched-work", repo: "myrepo"},
        ]"#,
    );

    let space = rk_space::Space::open_in_memory().unwrap();
    let scheduler = build_scheduler(&layout, SchedulerConfig::default(), space);

    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    assert_eq!(
        scheduler.run_cycle_at(t).unwrap(),
        0,
        "no-repo skipped, bad-cron skipped"
    );
    assert_eq!(scheduler.engine_instance_count(), 0);
}

/// A repo-local schedule file (`<repo>/.rk/schedules.cue`) defaults its target
/// repo to the repo it was discovered in — no explicit `repo:` needed.
#[tokio::test]
async fn repo_local_schedule_defaults_its_repo() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    register_repo(&layout, "myrepo", repo.path());
    let rk_dir = repo.path().join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("schedules.cue"),
        r#"schedules: [{name: "local-tick", cron: "* * * * *", run: "sched-work"}]"#,
    )
    .unwrap();

    let space = rk_space::Space::open_in_memory().unwrap();
    let scheduler = build_scheduler(&layout, SchedulerConfig::default(), space);

    let t = Utc.with_ymd_and_hms(2026, 7, 23, 8, 0, 0).unwrap();
    assert_eq!(
        scheduler.run_cycle_at(t).unwrap(),
        1,
        "repo-local schedule resolves to its own repo and fires"
    );
    assert_eq!(scheduler.engine_instance_count(), 1);
}
