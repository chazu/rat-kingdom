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
//!
//! A `Running` instance older than `stale_running_hours` (default 6h — above
//! rat p99 runtime, well below the 24h nightly cadence) no longer counts as a
//! block: a wedged instance would otherwise make its schedule skip forever.
//! The bypass is surfaced, not silent — a `need` tuple escalates the stale
//! instance so a human can investigate the wedge.

use crate::repos::RepoRegistry;
use crate::workflow_exec::{Instance, InstanceStatus, WorkflowEngine};
use crate::cron::Cron;
use rk_core::config::SchedulerConfig;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Tuple, DEFAULT_TRAIL_TTL, SYSTEM_SCOPE};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use rk_workflow::Schedule;
use serde_json::{json, Value};
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
    space: rk_space::Space,
    castle: String,
    /// Per-schedule cache: schedule name -> the instance id of its most recent
    /// fire. Durable running instances retain the schedule name, so a restart
    /// cannot clear the actual per-schedule single-flight guard.
    running: Mutex<HashMap<String, String>>,
}

const MAX_CATCHUP_MINUTES: u64 = 7 * 24 * 60;

impl Scheduler {
    pub fn new(
        engine: Arc<WorkflowEngine>,
        layout: Layout,
        config: SchedulerConfig,
        space: rk_space::Space,
        castle: String,
    ) -> Self {
        let cursor_file = layout.home().join("scheduler-cursor");
        Self {
            engine,
            layout,
            config,
            cursor_file,
            space,
            castle,
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
                let catchup = self.config.catchup_minutes.min(MAX_CATCHUP_MINUTES);
                let earliest = now_min
                    - ChronoDuration::minutes(i64::try_from(catchup).unwrap_or(i64::MAX));
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
                    match self.try_fire(loaded, &registry, now) {
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
    fn try_fire(&self, loaded: &Loaded, registry: &RepoRegistry, now: DateTime<Utc>) -> rk_core::Result<bool> {
        let sched = &loaded.schedule;

        // Fast path: skip if this process already remembers an active fire —
        // unless that run has gone stale (wedged past the staleness bound), in
        // which case it no longer blocks and is escalated instead.
        if let Some(id) = self.running.lock().unwrap_or_else(|p| p.into_inner()).get(&sched.name).cloned() {
            if let Some(instance) = self.engine.status(&id) {
                if instance.status == InstanceStatus::Running {
                    if self.is_stale(&instance, now) {
                        self.escalate_stale(loaded, &instance, now);
                    } else {
                        info!(schedule = %sched.name, instance = %id, "scheduler: previous run still active; skipping (single-flight)");
                        return Ok(false);
                    }
                }
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

        // Restart path: the in-memory schedule cache is empty after boot, but the
        // workflow snapshots have already been rehydrated. Prefer persisted
        // schedule identity. Legacy snapshots predate that field, so exact work
        // identity is their conservative compatibility fallback.
        if let Some(instance) = self.engine.list().into_iter().find(|instance| {
            instance.status == InstanceStatus::Running
                && instance.depth == 0
                && (instance.schedule.as_deref() == Some(sched.name.as_str())
                    || (instance.schedule.is_none()
                        && instance.repo == repo_path
                        && instance.definition == sched.run
                        && instance.params == params))
        }) {
            if self.is_stale(&instance, now) {
                self.escalate_stale(loaded, &instance, now);
            } else {
                info!(schedule = %sched.name, instance = %instance.id, "scheduler: durable matching run still active; skipping (single-flight)");
                self.running
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(sched.name.clone(), instance.id);
                return Ok(false);
            }
        }

        let instance = self
            .engine
            .run_scheduled(&sched.name, &sched.run, &repo_path, params)?;
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

    /// Whether a `Running` instance has aged past the staleness bound and
    /// should stop blocking its schedule's next fire.
    fn is_stale(&self, instance: &Instance, now: DateTime<Utc>) -> bool {
        let bound = ChronoDuration::hours(i64::try_from(self.config.stale_running_hours).unwrap_or(i64::MAX));
        now.signed_duration_since(instance.started_at) > bound
    }

    /// Surface a bypassed stale instance via a `need` tuple (which `rk inbox`
    /// ranks) instead of silently letting the schedule route around it forever.
    fn escalate_stale(&self, loaded: &Loaded, instance: &Instance, now: DateTime<Utc>) {
        let sched = &loaded.schedule;
        let age = now.signed_duration_since(instance.started_at);
        let age_hours = age.num_hours();
        warn!(
            schedule = %sched.name,
            instance = %instance.id,
            age_hours,
            bound_hours = self.config.stale_running_hours,
            "scheduler: previous run exceeds staleness bound; no longer blocking dispatch"
        );
        let scope = sched
            .repo
            .clone()
            .or_else(|| loaded.source_repo.clone())
            .unwrap_or_else(|| SYSTEM_SCOPE.to_string());
        let tuple = Tuple::new(
            Category::Need,
            scope,
            sched.name.clone(),
            self.castle.clone(),
            json!({
                "type": "schedule_stale_running",
                "schedule": sched.name,
                "instance": instance.id,
                "age_hours": age_hours,
                "bound_hours": self.config.stale_running_hours,
                "text": format!(
                    "schedule {} has a Running instance {} that is {}h old, over the {}h \
                     staleness bound; it no longer blocks dispatch — investigate the wedged run",
                    sched.name, instance.id, age_hours, self.config.stale_running_hours,
                ),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit schedule-stale-running need");
        }
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
