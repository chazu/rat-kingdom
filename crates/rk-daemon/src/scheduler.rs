//! The daemon scheduler: registered `#Schedule` definitions that fire workflows
//! on a cron cadence. The TIME axis of autonomy — where the [reactor] dispatches
//! on a matching tuple, the scheduler dispatches on a clock. A scheduled fire is
//! a time-sourced trigger: it resolves the target repo and calls `engine.run`,
//! the very same dispatch path the reactor uses.
//!
//! [reactor]: crate::reactor
//!
//! # Cursor discipline (and catch-up)
//!
//! The scheduler keeps a durable cursor: the last UTC minute it evaluated. Each
//! cycle it evaluates every minute in `(cursor, now]` — normally just the one
//! new minute — against each schedule's cron, firing a schedule at most once per
//! cycle even if several of its minutes elapsed. On first boot the cursor is
//! baselined to the current minute so no backlog fires. After downtime the
//! look-back is bounded by `catchup_minutes`, so a daemon down overnight runs
//! each missed daily/hourly schedule once, not a replay of every minute.
//!
//! # Single-flight
//!
//! Each schedule is guarded by its own single-flight lock keyed on the schedule
//! name: if its previous run's workflow instance is still `Running`, the fire is
//! skipped. So a slow nightly drain never stacks a second copy on itself.

use crate::repos::RepoRegistry;
use crate::workflow_exec::{InstanceStatus, WorkflowEngine};
use crate::cron::Cron;
use rk_core::config::SchedulerConfig;
use rk_core::paths::Layout;
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use rk_workflow::Schedule;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// A loaded schedule plus where it came from (a repo-local file defaults its
/// target repo to that repo; a global-dir schedule has no default repo).
struct Loaded {
    schedule: Schedule,
    source_repo: Option<String>,
}

pub struct Scheduler {
    engine: Arc<WorkflowEngine>,
    layout: Layout,
    config: SchedulerConfig,
    cursor_file: PathBuf,
    /// Per-schedule single-flight: schedule name -> the instance id of its most
    /// recent fire. In-memory: a restart legitimately clears it (the old run has
    /// almost certainly finished or been reaped), and the running-instance check
    /// is the actual guard.
    running: Mutex<HashMap<String, String>>,
}

impl Scheduler {
    pub fn new(engine: Arc<WorkflowEngine>, layout: Layout, config: SchedulerConfig) -> Self {
        let cursor_file = layout.home().join("scheduler-cursor");
        Self {
            engine,
            layout,
            config,
            cursor_file,
            running: Mutex::new(HashMap::new()),
        }
    }

    /// Baseline the cursor to the current minute so a fresh daemon does not fire
    /// schedules for minutes that elapsed before it started. A no-op once a
    /// cursor file exists (restarts resume where they left off, bounded by
    /// `catchup_minutes`).
    pub fn initialize_cursor(&self) -> rk_core::Result<()> {
        if self.cursor_file.exists() {
            return Ok(());
        }
        self.save_cursor(truncate_to_minute(Utc::now()))
    }

    /// Evaluate every minute newer than the cursor against each schedule and fire
    /// the workflows whose cron matches. Returns how many workflows were fired.
    pub fn run_cycle(&self) -> rk_core::Result<usize> {
        self.run_cycle_at(Utc::now())
    }

    /// [`run_cycle`](Self::run_cycle) with an injectable "now", for tests.
    pub fn run_cycle_at(&self, now: DateTime<Utc>) -> rk_core::Result<usize> {
        let now_min = truncate_to_minute(now);
        let cursor = self.load_cursor();
        // Nothing new until a fresh minute rolls over.
        let start = match cursor {
            Some(c) if c >= now_min => return Ok(0),
            Some(c) => {
                let earliest = now_min - ChronoDuration::minutes(self.config.catchup_minutes as i64);
                (c + ChronoDuration::minutes(1)).max(earliest)
            }
            // No cursor (initialize_cursor not run): evaluate only this minute.
            None => now_min,
        };

        let registry = RepoRegistry::load(&self.layout.home().join("repos.json"))?;
        // Pre-parse each schedule's cron once; a malformed expression is logged
        // and skipped, never fatal (mirrors a bad trigger file).
        let parsed: Vec<(Loaded, Cron)> = self
            .load_all_schedules(&registry)
            .into_iter()
            .filter_map(|loaded| match Cron::parse(&loaded.schedule.cron) {
                Ok(cron) => Some((loaded, cron)),
                Err(e) => {
                    warn!(schedule = %loaded.schedule.name, cron = %loaded.schedule.cron, error = %e, "scheduler: bad cron; skipping");
                    None
                }
            })
            .collect();

        let mut fired = 0usize;
        // A schedule fires at most once per cycle, no matter how many of its
        // minutes elapsed since the cursor (a missed nightly run runs once).
        let mut fired_names: HashSet<&str> = HashSet::new();
        let mut minute = start;
        while minute <= now_min {
            for (loaded, cron) in &parsed {
                if fired_names.contains(loaded.schedule.name.as_str()) {
                    continue;
                }
                if cron.matches(minute) {
                    fired_names.insert(loaded.schedule.name.as_str());
                    match self.try_fire(loaded, &registry) {
                        Ok(true) => fired += 1,
                        Ok(false) => {}
                        Err(e) => {
                            warn!(schedule = %loaded.schedule.name, error = %e, "scheduler dispatch failed")
                        }
                    }
                }
            }
            minute += ChronoDuration::minutes(1);
        }
        self.save_cursor(now_min)?;
        Ok(fired)
    }

    /// Resolve the target repo, apply the single-flight guard, and dispatch.
    /// Returns whether a workflow was actually fired.
    fn try_fire(&self, loaded: &Loaded, registry: &RepoRegistry) -> rk_core::Result<bool> {
        let sched = &loaded.schedule;

        // Single-flight: skip if this schedule's previous run is still active.
        if let Some(id) = self.running.lock().unwrap_or_else(|p| p.into_inner()).get(&sched.name) {
            if matches!(
                self.engine.status(id).map(|i| i.status),
                Some(InstanceStatus::Running)
            ) {
                info!(schedule = %sched.name, instance = %id, "scheduler: previous run still active; skipping (single-flight)");
                return Ok(false);
            }
        }

        // Target repo: explicit override > the schedule file's own repo. Unlike a
        // trigger there is no tuple scope to fall back on, so a global schedule
        // with no repo cannot resolve.
        let Some(repo_name) = sched.repo.clone().or_else(|| loaded.source_repo.clone()) else {
            warn!(schedule = %sched.name, "scheduler: no repo (global schedule must set `repo`); skipping");
            return Ok(false);
        };
        let Some(record) = registry.get(&repo_name) else {
            warn!(schedule = %sched.name, repo = %repo_name, "scheduler: no such registered repo; skipping");
            return Ok(false);
        };
        let repo_path = record.path.to_string_lossy().to_string();
        let params: HashMap<String, Value> = sched
            .params
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();

        let instance = self.engine.run(&sched.run, &repo_path, params)?;
        info!(
            schedule = %sched.name,
            workflow = %sched.run,
            instance = %instance.id,
            cron = %sched.cron,
            "scheduler fired workflow"
        );
        self.running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(sched.name.clone(), instance.id);
        Ok(true)
    }

    /// Discover schedules from the global dir and each registered repo's
    /// `.rk/schedules.cue`. A malformed file is logged and skipped, never fatal.
    fn load_all_schedules(&self, registry: &RepoRegistry) -> Vec<Loaded> {
        let mut out = Vec::new();
        for file in rk_workflow::definitions(&self.layout.schedules_dir()) {
            match rk_workflow::load_schedules(&file) {
                Ok(ss) => out.extend(ss.into_iter().map(|schedule| Loaded {
                    schedule,
                    source_repo: None,
                })),
                Err(e) => {
                    warn!(file = %file.display(), error = %e, "scheduler: bad global schedule file")
                }
            }
        }
        for repo in registry.list() {
            let file = repo.path.join(".rk").join("schedules.cue");
            if !file.exists() {
                continue;
            }
            match rk_workflow::load_schedules(&file) {
                Ok(ss) => out.extend(ss.into_iter().map(|schedule| Loaded {
                    schedule,
                    source_repo: Some(repo.name.clone()),
                })),
                Err(e) => {
                    warn!(repo = %repo.name, error = %e, "scheduler: bad repo schedule file")
                }
            }
        }
        out
    }

    /// Test-only: how many workflow instances the fired workflows created.
    #[doc(hidden)]
    pub fn engine_instance_count(&self) -> usize {
        self.engine.list().len()
    }

    fn load_cursor(&self) -> Option<DateTime<Utc>> {
        let raw = std::fs::read_to_string(&self.cursor_file).ok()?;
        DateTime::parse_from_rfc3339(raw.trim())
            .ok()
            .map(|d| d.with_timezone(&Utc))
    }

    fn save_cursor(&self, minute: DateTime<Utc>) -> rk_core::Result<()> {
        std::fs::write(&self.cursor_file, minute.to_rfc3339())?;
        Ok(())
    }
}

/// Truncate an instant to its minute (seconds/nanos zeroed) in UTC.
fn truncate_to_minute(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_second(0)
        .and_then(|d| d.with_nanosecond(0))
        .unwrap_or(dt)
}
