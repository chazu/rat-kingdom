//! The supervisor: spawn rats into worktrees, pump their harness events into
//! the registry and tuplespace, route completions up the spawn tree, and
//! merge their work on dismissal.

use crate::agents::{AgentRecord, AgentState, Registry};
use chrono::{DateTime, Utc};
use rk_core::config::{MergeMode, SupervisorConfig};
use rk_core::paths::Layout;
use rk_core::prime::{render, PrimeContext};
use rk_core::tuple::{Category, Pattern, Tuple, DEFAULT_TRAIL_TTL, SYSTEM_SCOPE};
use rk_git::{agent_branch, Repo};
use rk_harness::{make_harness, HarnessEvent, LaunchSpec, SessionControl, TokenUsage};
use rk_ledger::pricing::PricingTable;
use rk_ledger::{Budget, BudgetAction, BudgetScope, DispatchCheck, FleetBudget};
use rk_space::Space;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Debug, Clone, Deserialize)]
pub struct SpawnParams {
    /// Path to the repository (or any path inside it).
    pub repo: String,
    /// Task identifier.
    pub task: String,
    /// Task description / initial prompt body.
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default = "default_role")]
    pub role: String,
    /// Harness kind; falls back to the daemon's configured default.
    #[serde(default)]
    pub harness: Option<String>,
    /// Spawning agent (structural parent for completion routing).
    #[serde(default)]
    pub parent: Option<String>,
    /// Base/merge-target branch; defaults to the repo's current branch.
    #[serde(default)]
    pub base: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
    /// Run in a herdr pane (interactive, human-attachable) instead of
    /// headless. Completion comes from the rat's own `rk done` tuple.
    #[serde(default)]
    pub attach: bool,
    /// Workflow instance dispatching this spawn (None = not from a workflow).
    /// Recorded on the agent so its cost sums into the instance's rollup.
    #[serde(default)]
    pub workflow_instance: Option<String>,
    /// Per-instance USD cap for `workflow_instance`, from the workflow's
    /// `budget:` field. Enforced as a dispatch preflight: once this instance's
    /// summed cost reaches it, further spawns are refused. `None`/0 = unlimited.
    #[serde(default)]
    pub instance_max_usd: Option<f64>,
}

fn default_role() -> String {
    "rat".into()
}

/// Render one budget scope's rollup for `rk cost --fleet`: spend vs cap, the
/// remaining headroom, and an "ok"/"warn"/"exceeded"/"unlimited" status. A
/// `repo` label is attached for per-repo rows and omitted for the fleet total.
fn scope_json(spent: f64, cap: f64, warn_at: f64, repo: Option<String>) -> serde_json::Value {
    let warn_frac = if warn_at > 0.0 { warn_at } else { 0.8 };
    let status = if cap <= 0.0 {
        "unlimited"
    } else if spent >= cap {
        "exceeded"
    } else if spent >= cap * warn_frac {
        "warn"
    } else {
        "ok"
    };
    let mut obj = json!({
        "spent_usd": spent,
        "cap_usd": cap,
        "remaining_usd": if cap > 0.0 { (cap - spent).max(0.0) } else { 0.0 },
        "status": status,
    });
    if let Some(repo) = repo {
        obj["repo"] = json!(repo);
    }
    obj
}

pub struct Supervisor {
    layout: Layout,
    castle: String,
    default_harness: String,
    registry: Mutex<Registry>,
    /// Live control handles (not persisted; gone after restart).
    controls: Mutex<HashMap<String, SessionControl>>,
    space: Space,
    /// Shared with the server so ticket-lifecycle writes serialize on one lock.
    tickets: Arc<crate::tickets::Tickets>,
    pricing: PricingTable,
    budget: Budget,
    /// Hierarchical fleet/repo caps enforced as a pre-dispatch guard.
    fleet_budget: FleetBudget,
    /// Agents already warned about budget (avoid repeat warnings).
    budget_warned: Mutex<std::collections::HashSet<String>>,
    /// Fleet/repo scopes already warned at dispatch (avoid repeat obstacles).
    fleet_warned: Mutex<std::collections::HashSet<String>>,
    /// Per-agent liveness-sweep bookkeeping (burn-rate deltas + flag episodes).
    sweep_state: Mutex<HashMap<String, SweepState>>,
    /// Per-agent self-healing-respawn bookkeeping (attempt count + backoff
    /// clock + whether the cap has already been escalated). In-memory: a daemon
    /// restart is a fresh episode, so attempt counts reset with it.
    respawn_state: Mutex<HashMap<String, RespawnState>>,
    /// Per-agent completion-routing state, so one agent generation emits
    /// exactly one durable `harness_result` — the one for the turn it actually
    /// finished on (TKT-160). In-memory: a daemon restart loses the withheld
    /// turn, which is the same blindness a restart already imposes on the
    /// harness event stream it came from.
    completions: Mutex<HashMap<String, CompletionState>>,
    /// Bounded per-agent transcript (assistant text / tool calls / retries),
    /// so the operator can `rk log` a run instead of being blind to it.
    log: crate::agent_log::AgentLog,
    /// Serializes concurrent land/dismiss merges to the same target branch so
    /// unattended auto-merges never interleave and lose a branch (TKT-51).
    merge_queue: MergeQueue,
}

/// How far one agent generation has got through reporting its completion.
///
/// A harness returns control once per TURN, not once per task: a re-armed
/// monitor, a background test suite finishing, or a task notification all end a
/// turn and produce a `Completed` event while the rat is still mid-task. Every
/// one of those used to be published as a durable `harness_result`, and every
/// reader of that event — the workflow `wait`, the reactor's steward trigger,
/// the ticket auto-close — takes the OLDEST match, i.e. the mid-flight one.
/// This is the bookkeeping that reduces a generation's turns to the single turn
/// it finished on. See [`Supervisor::claim_completion`].
#[derive(Debug, Clone)]
struct CompletionState {
    /// Which generation of the name this state describes (the record's
    /// `created_at`). A later generation resets it.
    generation: DateTime<Utc>,
    /// This generation's `harness_result` has been published; later turns of
    /// the same generation must not publish a second one.
    routed: bool,
    /// A clean turn result is being held back because nothing yet proves the
    /// session is over. Superseded by each later turn, and flushed if the
    /// process exits (see the `Exited` arm of [`Supervisor::handle_event`]).
    withheld: bool,
}

/// What [`Supervisor::claim_completion`] decided about the turn that just ended.
#[derive(Debug, Clone, Copy)]
struct TurnClaim {
    /// Publish this turn as the generation's `harness_result`.
    publish: bool,
    /// This generation wrote its `rk done` — see [`Supervisor::declared_done`].
    /// Carried onto the published event so a reader can tell a rat that
    /// declared itself finished from one that merely stopped producing turns.
    declared_done: bool,
}

/// One agent's rolling state across supervisor sweeps.
#[derive(Debug, Clone)]
struct SweepState {
    /// Cost at the previous sweep, for the burn-rate delta.
    last_cost_usd: f64,
    /// When the previous sweep observed this agent (burn-rate denominator).
    last_observed: DateTime<Utc>,
    /// When the current STUCK/RUNAWAY episode was first flagged (soft-steered).
    /// `None` = not currently flagged. The kill escalation measures from here.
    flagged_at: Option<DateTime<Utc>>,
}

/// What a sweep decided to do about one agent, computed under the sweep-state
/// lock and then acted on after releasing it (steer/kill spawn async tasks).
enum SweepAction {
    /// Healthy, or still within the grace window — leave it alone.
    None,
    /// First detection this episode: obstacle tuple + soft steer.
    Soft { kind: &'static str, detail: String },
    /// Still flagged past the grace window: obstacle tuple + kill.
    Hard { kind: &'static str, detail: String },
}

/// One crashed agent's rolling self-healing-respawn state across sweeps.
#[derive(Debug, Clone)]
struct RespawnState {
    /// How many auto-respawns have fired for this agent this daemon lifetime.
    attempts: u32,
    /// When the last auto-respawn fired — the exponential-backoff clock.
    last_attempt: DateTime<Utc>,
    /// Set once the attempt cap has been hit and a `need` escalated, so the
    /// sweep does not re-escalate the same exhausted agent every cycle.
    escalated: bool,
}

/// What the respawn sweep decided to do about one crashed agent, computed under
/// the respawn-state lock and acted on after it is released (respawn launches).
enum RespawnDecision {
    /// Within the backoff window (or nothing to do) — leave it for next sweep.
    Wait,
    /// Backoff elapsed and attempts remain — relaunch it in its worktree.
    Respawn,
    /// Attempt cap exhausted — escalate a `need` for a human, once.
    Escalate,
}

/// Serializes merges to the same target branch — the land / merge queue.
///
/// Both the steward's `land` step and `dismiss`/`dismiss_all` merge branches
/// into `main` (or any base) concurrently and unattended. Without
/// serialization two auto-merges racing on the same target interleave: each
/// merges in its own detached worktree captured from the target ref *before*
/// the other advanced it, so the compare-and-swap in [`Repo::merge_branch`]
/// bounces the loser to a silent `merged: false` and its branch is left
/// unmerged (the root cause of the "done ticket never in main" gap). A
/// per-`(repo, target)` FIFO lock makes every land/dismiss to a given target
/// take its turn on the *freshly-updated* target, so each merge either applies
/// cleanly or is a genuine conflict — never a lost race. Merges to distinct
/// targets keep separate locks and still run concurrently.
#[derive(Default)]
struct MergeQueue {
    /// One async mutex per active target key; entries are created on demand.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl MergeQueue {
    fn key(repo_root: &std::path::Path, target: &str) -> String {
        // NUL can't appear in a path or ref name, so it's an unambiguous joiner.
        format!("{}\u{0}{}", repo_root.display(), target)
    }

    /// Acquire the serialization lock for `(repo_root, target)`. The returned
    /// guard is held for the duration of one merge; the next waiter proceeds
    /// only once it drops. tokio's `Mutex` is FIFO, so callers land in arrival
    /// order — the "queue" in merge queue.
    async fn acquire(
        &self,
        repo_root: &std::path::Path,
        target: &str,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let key = Self::key(repo_root, target);
        // Clone the Arc out under the std mutex, then release it *before*
        // awaiting the async lock — never hold a std guard across an await.
        let lock = {
            let mut locks = self.locks.lock().unwrap();
            Arc::clone(
                locks
                    .entry(key)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        lock.lock_owned().await
    }
}

/// Which of an archived record's leftovers `rk prune` should reclaim, beyond
/// the record itself. Named fields rather than two positional `bool`s, because
/// silently swapping them is exactly the bug worth designing out.
///
/// Two switches and not one, because they answer different questions. A branch
/// can still hold the only copy of a rat's work, so `git` defers to whether it
/// has landed; a transcript only narrates work that lives elsewhere, so `logs`
/// has nothing to defer to. Folding them together would mean either deleting
/// the transcript of the very rat whose branch was kept back for a closer look,
/// or never reaping the transcript of a rat that had no branch at all.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reap {
    /// The worktree and local branch — merged branches only.
    pub git: bool,
    /// The generation's transcript file under `agent-logs/`. One-way.
    pub logs: bool,
}

impl Supervisor {
    pub fn new(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: Budget,
        fleet_budget: FleetBudget,
        space: Space,
        tickets: Arc<crate::tickets::Tickets>,
    ) -> rk_core::Result<Self> {
        let registry = Registry::load(&layout.home().join("agents.json"))?;
        let mut pricing = PricingTable::vendored();
        // User/runtime overrides, LiteLLM-shaped.
        let overrides = layout.home().join("pricing.json");
        if let Ok(json) = std::fs::read_to_string(&overrides) {
            match pricing.merge_pricing_json(&json) {
                Ok(n) => tracing::info!(entries = n, "merged pricing overrides"),
                Err(e) => warn!(error = %e, "invalid pricing.json ignored"),
            }
        }
        let log = crate::agent_log::AgentLog::new(&layout);
        Ok(Self {
            layout,
            castle,
            default_harness,
            registry: Mutex::new(registry),
            controls: Mutex::new(HashMap::new()),
            space,
            tickets,
            pricing,
            budget,
            fleet_budget,
            budget_warned: Mutex::new(std::collections::HashSet::new()),
            fleet_warned: Mutex::new(std::collections::HashSet::new()),
            sweep_state: Mutex::new(HashMap::new()),
            respawn_state: Mutex::new(HashMap::new()),
            completions: Mutex::new(HashMap::new()),
            log,
            merge_queue: MergeQueue::default(),
        })
    }

    /// The per-agent transcript store (for `agent.log` reads and `--follow`).
    pub fn log(&self) -> &crate::agent_log::AgentLog {
        &self.log
    }

    /// Every transcript generation of `name`, oldest first, each carrying the
    /// exclusive upper bound (the next generation's `created_at`) that isolates
    /// it inside a legacy name-keyed log file.
    ///
    /// Empty when no record — live or archived — carries the name. Callers
    /// reading a transcript anyway should fall back to
    /// [`Generation::unrecorded`](crate::agent_log::Generation::unrecorded).
    pub fn log_generations(&self, name: &str) -> Vec<crate::agent_log::Generation> {
        let starts = self.lock_registry().generations_of(name);
        starts
            .iter()
            .enumerate()
            .map(|(i, &start)| {
                crate::agent_log::Generation::of(name, start, starts.get(i + 1).copied())
            })
            .collect()
    }

    /// Called once the daemon has WON the socket bind — never earlier. A
    /// Daemon that loses the bind race must not touch shared registry state.
    pub fn on_daemon_started(&self) {
        match self.lock_registry().orphan_live_agents() {
            Ok(orphaned) if !orphaned.is_empty() => {
                warn!(?orphaned, "orphaned live agents from previous daemon run");
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "failed to orphan stale agents"),
        }
    }

    pub fn spawn(self: &Arc<Self>, params: SpawnParams) -> rk_core::Result<AgentRecord> {
        let repo = Repo::discover(std::path::Path::new(&params.repo))?;
        let repo_name = repo.name();

        // Hierarchical fleet/repo budget guard: the wallet kill-switch. Once the
        // fleet-wide (or per-repo) cost sum reaches its cap we refuse the spawn
        // here — before any worktree/branch/name is allocated — so a runaway
        // autoscaler or nightly drain stops dispatching instead of draining the
        // account. Single spawns and workflow fan-out both funnel through here.
        // A workflow's per-instance cap is enforced in the same preflight.
        self.check_dispatch_budget(
            &repo_name,
            params.workflow_instance.as_deref(),
            params.instance_max_usd,
        )?;
        let target_branch = match &params.base {
            Some(b) => b.clone(),
            None => repo.current_branch()?,
        };

        // Reserve the name atomically: it stays claimed against concurrent
        // spawns until `insert` records the rat (or a failure path below frees
        // it). Picking without reserving let two near-simultaneous spawns grab
        // the same name and collide on the worktree path.
        let name = self.lock_registry().reserve_name();
        let branch = agent_branch(&name, &params.task);
        let worktree = self.layout.worktrees_dir().join(&repo_name).join(&name);
        if let Err(e) = repo.create_worktree(&worktree, &branch, &target_branch) {
            self.lock_registry().release_name(&name);
            return Err(e);
        }

        let harness_kind = params
            .harness
            .clone()
            .unwrap_or_else(|| self.default_harness.clone());
        let harness = match make_harness(&harness_kind) {
            Ok(h) => h,
            Err(e) => {
                let _ = repo.remove_worktree(&worktree);
                let _ = repo.delete_branch(&branch);
                self.lock_registry().release_name(&name);
                return Err(e);
            }
        };

        let prime_ctx = PrimeContext {
            agent: name.clone(),
            repo: repo_name.clone(),
            task: Some(params.task.clone()),
            branch: Some(branch.clone()),
            parent: params.parent.clone(),
            conventions: self.scan_conventions(&repo_name),
        };
        let prompt = params
            .prompt
            .clone()
            .unwrap_or_else(|| format!("Work on task {}. Begin now.", params.task));

        let mut env = self.agent_env(
            &name,
            &params.role,
            &repo_name,
            &params.task,
            Some(&branch),
            &worktree,
        );
        if let Some(parent) = &params.parent {
            env.insert("RK_PARENT".into(), parent.clone());
        }

        let spec = LaunchSpec {
            prompt,
            system_prompt: Some(render(&params.role, &prime_ctx)),
            cwd: worktree.clone(),
            env,
            // Rats work in isolated worktrees; autonomous operation is the
            // default. Override per-spawn for tighter modes.
            permission_mode: Some(
                params
                    .permission_mode
                    .clone()
                    .unwrap_or_else(|| "bypassPermissions".into()),
            ),
            model: params.model.clone(),
            resume_session: None,
        };

        if params.attach {
            return self.spawn_attached(params, repo, repo_name, name, branch, worktree, spec);
        }

        let session = match harness.launch(&spec) {
            Ok(s) => s,
            Err(e) => {
                let _ = repo.remove_worktree(&worktree);
                let _ = repo.delete_branch(&branch);
                self.lock_registry().release_name(&name);
                return Err(e);
            }
        };

        let record = AgentRecord {
            name: name.clone(),
            role: params.role.clone(),
            harness: harness_kind,
            model: params.model.clone(),
            repo_root: repo.root().to_path_buf(),
            repo_name: repo_name.clone(),
            task: Some(params.task.clone()),
            branch: Some(branch),
            worktree: Some(worktree),
            target_branch,
            parent: params.parent.clone(),
            workflow_instance: params.workflow_instance.clone(),
            session_id: None,
            attach_target: None,
            pid: session.pid,
            merge_commit: None,
            state: AgentState::Running,
            crashed: false,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
        };
        self.lock_registry().insert(record.clone())?;
        self.lock_controls()
            .insert(name.clone(), session.control.clone());

        self.emit_event(
            &repo_name,
            "agent_spawned",
            json!({"agent": name, "task": params.task, "role": params.role, "parent": params.parent}),
        );

        let supervisor = Arc::clone(self);
        let mut events = session.events;
        let generation = record.created_at;
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&name, generation, event);
            }
        });

        Ok(record)
    }

    /// Attach-mode spawn: the harness runs interactively in a herdr pane.
    /// The daemon still owns the worktree/branch/registry; completion arrives
    /// via the rat's own `rk done` tuple, and humans can attach any time.
    #[allow(clippy::too_many_arguments)]
    fn spawn_attached(
        self: &Arc<Self>,
        params: SpawnParams,
        repo: Repo,
        repo_name: String,
        name: String,
        branch: String,
        worktree: std::path::PathBuf,
        spec: LaunchSpec,
    ) -> rk_core::Result<AgentRecord> {
        if !rk_mux::HerdrMux::available() {
            let _ = repo.remove_worktree(&worktree);
            let _ = repo.delete_branch(&branch);
            self.lock_registry().release_name(&name);
            return Err(rk_core::Error::other(
                "--attach needs a running herdr server (https://herdr.dev); \
                 spawn headless or start herdr first",
            ));
        }
        let harness_kind = params
            .harness
            .clone()
            .unwrap_or_else(|| self.default_harness.clone());
        let argv = rk_mux::interactive_argv(
            &harness_kind,
            spec.system_prompt.as_deref(),
            spec.model.as_deref(),
            spec.permission_mode.as_deref(),
        )?;
        let target = match rk_mux::HerdrMux::start_agent(&name, &worktree, &spec.env, &argv) {
            Ok(t) => t,
            Err(e) => {
                let _ = repo.remove_worktree(&worktree);
                let _ = repo.delete_branch(&branch);
                self.lock_registry().release_name(&name);
                return Err(e);
            }
        };

        let record = AgentRecord {
            name: name.clone(),
            role: params.role.clone(),
            harness: harness_kind,
            model: params.model.clone(),
            repo_root: repo.root().to_path_buf(),
            repo_name: repo_name.clone(),
            task: Some(params.task.clone()),
            branch: Some(branch),
            worktree: Some(worktree),
            target_branch: match &params.base {
                Some(b) => b.clone(),
                None => repo.current_branch()?,
            },
            parent: params.parent.clone(),
            workflow_instance: params.workflow_instance.clone(),
            session_id: None,
            attach_target: Some(target.clone()),
            pid: None,
            merge_commit: None,
            state: AgentState::Running,
            crashed: false,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
        };
        self.lock_registry().insert(record.clone())?;
        self.emit_event(
            &repo_name,
            "agent_spawned",
            json!({"agent": name, "task": params.task, "role": params.role, "attached": true}),
        );

        // Deliver the initial prompt once herdr reports the TUI idle (its
        // integration hook, not a sleep); fall back to sending anyway.
        {
            let target = target.clone();
            let prompt = spec.prompt.clone();
            tokio::task::spawn_blocking(move || {
                let _ = std::process::Command::new("herdr")
                    .args([
                        "agent",
                        "wait",
                        &target,
                        "--status",
                        "idle",
                        "--timeout",
                        "30000",
                    ])
                    .output();
                if let Err(e) = rk_mux::HerdrMux::send(&target, &prompt) {
                    warn!(error = %e, "failed to deliver prompt to herdr pane");
                }
            });
        }

        // Completion watcher: the rat's `rk done` tuple is the signal.
        {
            let supervisor = Arc::clone(self);
            let agent = name.clone();
            let space = self.space.clone();
            // Bound the read to this generation of the name. `task_done` events
            // are durable and outlive the rat they name, so an unbounded name
            // search matches a PREDECESSOR's `rk done` and reports this rat
            // complete the instant it starts — the attach-mode twin of the
            // TKT-146 workflow-wait bug. `for_agent_since` is the one shared
            // constructor for that predicate (TKT-159); it has no unbounded form.
            let since = record.created_at;
            tokio::spawn(async move {
                let pattern = Pattern::for_agent_since(Category::Event, "task_done", &agent, since);
                match space
                    .rd(&pattern, std::time::Duration::from_secs(24 * 3600))
                    .await
                {
                    Ok(Some(tuple)) => {
                        let updated = supervisor.lock_registry().update(&agent, |r| {
                            r.state = AgentState::Completed;
                            r.result = tuple.payload["summary"]
                                .as_str()
                                .map(String::from)
                                .or(Some("done".into()));
                        });
                        if let Ok(Some(record)) = updated {
                            // Driven by the rat's own `task_done`, so this one is
                            // declared by construction.
                            supervisor.route_completion(&record, false, true);
                            rk_mux::HerdrMux::notify(
                                &format!("{agent} finished"),
                                record.result.as_deref().unwrap_or(""),
                            );
                        }
                    }
                    Ok(None) => {
                        warn!(agent = %agent, "attach-mode completion watch timed out");
                    }
                    Err(e) => warn!(error = %e, "completion watch failed"),
                }
            });
        }

        Ok(record)
    }

    /// Resume an orphaned/failed agent in its preserved worktree.
    pub fn respawn(self: &Arc<Self>, name: &str) -> rk_core::Result<AgentRecord> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        if record.state.is_live() {
            return Err(rk_core::Error::other(format!("{name} is still running")));
        }
        let (Some(worktree), Some(task)) = (record.worktree.clone(), record.task.clone()) else {
            return Err(rk_core::Error::other("record lacks worktree/task"));
        };

        let harness = make_harness(&record.harness)?;
        let resume = if harness.caps().resume {
            record.session_id.clone()
        } else {
            None
        };

        let env = self.agent_env(
            &record.name,
            &record.role,
            &record.repo_name,
            &task,
            record.branch.as_deref(),
            &worktree,
        );

        let prime_ctx = PrimeContext {
            agent: record.name.clone(),
            repo: record.repo_name.clone(),
            task: record.task.clone(),
            branch: record.branch.clone(),
            parent: record.parent.clone(),
            conventions: self.scan_conventions(&record.repo_name),
        };
        let spec = LaunchSpec {
            prompt: format!(
                "You are resuming task {task} after an interruption. Check `git log` and \
                 `git status` in your worktree to see where you left off, then continue. \
                 Finish with `rk done` as usual."
            ),
            system_prompt: Some(render(&record.role, &prime_ctx)),
            cwd: worktree,
            env,
            permission_mode: Some("bypassPermissions".into()),
            model: None,
            resume_session: resume,
        };
        let session = harness.launch(&spec)?;

        let updated = self
            .lock_registry()
            .update(name, |r| {
                r.state = AgentState::Running;
                r.pid = session.pid;
                r.result = None;
                // A fresh attempt: the previous crash no longer describes this
                // record, so a workflow waiting on it stops treating it as
                // abandoned (TKT-147).
                r.crashed = false;
            })?
            .ok_or_else(|| rk_core::Error::other("record vanished"))?;
        self.lock_controls()
            .insert(name.to_string(), session.control.clone());

        self.emit_event(
            &updated.repo_name,
            "agent_respawned",
            json!({"agent": name, "task": updated.task}),
        );

        // The interrupted run's completion bookkeeping does not carry over: its
        // withheld turn is stale, and (for a manually respawned Completed
        // record) its `routed` flag would gag the resumed run (TKT-160).
        self.forget_completion(name);

        let supervisor = Arc::clone(self);
        let owned = name.to_string();
        let mut events = session.events;
        // A respawn continues the SAME generation — the record (and its
        // `created_at`) is reused — so the second run appends to the transcript
        // the first run started, which is what an operator expects.
        let generation = updated.created_at;
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&owned, generation, event);
            }
        });
        Ok(updated)
    }

    /// `generation` is the agent record's `created_at`, captured once when the
    /// event loop is wired up: transcript writes are keyed on the generation, not
    /// the name, so a line can never land in a namesake's file.
    fn handle_event(
        self: &Arc<Self>,
        name: &str,
        generation: DateTime<Utc>,
        event: HarnessEvent,
    ) {
        match event {
            HarnessEvent::Started { session_id } => {
                let _ = self.lock_registry().update(name, |r| {
                    r.session_id = session_id.clone();
                });
            }
            HarnessEvent::Usage { usage } => {
                let updated = self.lock_registry().update(name, |r| {
                    r.usage.add(&usage);
                    // Incremental cost for harnesses that don't self-report
                    // USD; an authoritative Completed cost overwrites later.
                    if let Some(model) = &r.model {
                        if let Some(price) = self.pricing.lookup(model) {
                            r.cost_usd += price.cost(&usage);
                        }
                    }
                });
                if let Ok(Some(record)) = updated {
                    self.enforce_budget(&record);
                }
            }
            HarnessEvent::Completed {
                result,
                is_error,
                usage,
                cost_usd,
                session_id,
            } => {
                let updated = self.lock_registry().update(name, |r| {
                    r.state = if is_error {
                        AgentState::Failed
                    } else {
                        AgentState::Completed
                    };
                    r.result = Some(result.clone());
                    if usage.total() > 0 {
                        r.usage = usage;
                    }
                    if let Some(cost) = cost_usd {
                        r.cost_usd = cost;
                    }
                    if session_id.is_some() {
                        r.session_id = session_id.clone();
                    }
                });
                if let Ok(Some(record)) = updated {
                    let claim = self.claim_completion(name, generation, is_error);
                    if claim.publish {
                        info!(agent = name, is_error, "agent completed");
                        self.route_completion(&record, is_error, claim.declared_done);
                    } else {
                        info!(
                            agent = name,
                            "harness returned control without a `rk done`; holding the turn \
                             result back rather than publishing it as the completion"
                        );
                    }
                }
            }
            HarnessEvent::Exited { code } => {
                self.lock_controls().remove(name);
                let updated = self.lock_registry().update(name, |r| {
                    r.pid = None;
                    // Exit without a Completed event = crash/kill.
                    if r.state.is_live() {
                        r.state = AgentState::Failed;
                        // The one place that knows the harness never reported a
                        // verdict for this generation, so no `harness_result`
                        // exists or ever will. Recorded as data rather than
                        // left to be inferred from the result string, because a
                        // workflow `wait`/`evaluate` has to be able to tell a
                        // rat that produced nothing from one that ran (TKT-147).
                        r.crashed = true;
                        r.result =
                            Some(format!("process exited (code {code:?}) without completing"));
                    }
                });
                // The process is gone, so no further turn can follow: a turn
                // result held back for want of a `rk done` is now provably this
                // generation's last word, and must be published. Harnesses that
                // end with the run (codex, axe, the test fake) take this path
                // for every agent; a Claude session — which stays alive between
                // turns to receive steers — normally reports at its `rk done`
                // and only lands here when it is killed mid-task.
                //
                // Anything flushed here publishes as a FAILURE, whatever the
                // exit status (TKT-175). Reaching this flush is itself the
                // proof: a turn result is withheld for exactly one reason —
                // the generation had not written its `task_done` — so the text
                // being published is a mid-flight turn ("the test suite is
                // still running"), and by the fleet's own completion protocol a
                // rat that never declared itself done did not finish its task.
                // The honest publication is `is_error: true`.
                //
                // TKT-173 shipped the narrower half of this, keyed on whether
                // the process was KILLED (`status.code()` is `None` for a
                // signal-terminated child, and both the budget hard-stop and the
                // sweep's hard escalation SIGTERM the process group). That was
                // the loudest case: the mid-flight turn had already set the
                // record's state to `Completed`, so the kill did not read as a
                // crash either — TKT-147's `crashed` is only set over a LIVE
                // state — and a rat stopped by the budget reported
                // `is_error: false` to the workflow waiting on it.
                //
                // What survived was the same lie told more quietly: a rat that
                // exited 0 mid-task — every codex/axe run, whose harness ends
                // with the run rather than staying alive between turns — also
                // published `is_error: false`, and every workflow gating on
                // `expect {is_error: false}` accepted an unfinished task. How
                // the process ended is not the question; whether the rat said it
                // was finished is.
                //
                // The turn's TEXT is still published as-is, so `rk inbox` shows
                // what the rat had got to; only the verdict changes. A rat that
                // did declare done never reaches here at all — `claim_completion`
                // published its turn when it ended — which is what keeps this
                // from collapsing into "every fake-harness agent fails".
                if let Ok(Some(record)) = updated {
                    if self.flush_withheld_completion(name, generation) {
                        info!(
                            agent = name,
                            killed = code != Some(0),
                            exit_code = ?code,
                            "agent ended without ever running `rk done`; publishing its last \
                             turn result as a failure"
                        );
                        self.route_completion(&record, true, false);
                    }
                }
            }
            // Formerly dropped on the floor; now persisted as the agent's
            // transcript so the operator can `rk log` a run without --attach.
            HarnessEvent::AssistantText { text } => {
                self.log
                    .append(name, generation, crate::agent_log::LogEvent::Text { text });
            }
            HarnessEvent::ToolUse { name: tool } => {
                self.log.append(
                    name,
                    generation,
                    crate::agent_log::LogEvent::Tool { name: tool },
                );
            }
            HarnessEvent::Retry { attempt, error } => {
                self.log.append(
                    name,
                    generation,
                    crate::agent_log::LogEvent::Retry { attempt, error },
                );
            }
        }
    }

    /// Graduated budget policy: warn once at the threshold (obstacle tuple +
    /// steer when possible), hard-stop at the cap.
    fn enforce_budget(self: &Arc<Self>, record: &AgentRecord) {
        match self.budget.check(record.cost_usd, record.usage.total()) {
            BudgetAction::Ok => {}
            BudgetAction::Warn => {
                if !self.mark_budget_warned(&record.name) {
                    return; // already warned
                }
                warn!(agent = %record.name, cost = record.cost_usd, "budget warning threshold crossed");
                self.emit_obstacle_for_budget(record, "warning");
                let control = self.lock_controls().get(&record.name).cloned();
                let name = record.name.clone();
                if let Some(control) = control {
                    tokio::spawn(async move {
                        let _ = control
                            .steer(&format!(
                                "BUDGET WARNING for {name}: you are approaching your \
                                 token/cost cap. Wrap up: commit what you have and run \
                                 `rk done` now."
                            ))
                            .await;
                    });
                }
            }
            BudgetAction::Stop => {
                warn!(agent = %record.name, cost = record.cost_usd, tokens = record.usage.total(), "budget cap hit — stopping agent");
                self.emit_obstacle_for_budget(record, "exceeded");
                let control = self.lock_controls().remove(&record.name);
                if let Some(control) = control {
                    tokio::spawn(async move {
                        let _ = control.kill().await;
                    });
                }
            }
        }
    }

    /// Returns true if this call newly marked the agent (first warning).
    fn mark_budget_warned(&self, name: &str) -> bool {
        match self.budget_warned.lock() {
            Ok(mut set) => set.insert(name.to_string()),
            Err(p) => p.into_inner().insert(name.to_string()),
        }
    }

    /// Active fleet conventions binding on a rat spawned into `repo`: the text
    /// of every `Convention` tuple in the `system` scope (fleet-wide norms) and
    /// the repo's own scope (repo-local norms). Composed into the rat's prompt
    /// as a "Standing conventions" section (stigmergy P6) so a quorum-promoted
    /// norm actually changes what the rat does. Scan/parse failures degrade to
    /// no conventions — priming must never fail on a convention read.
    fn scan_conventions(&self, repo: &str) -> Vec<String> {
        let mut texts = Vec::new();
        for scope in [SYSTEM_SCOPE, repo] {
            let pattern = Pattern::category(Category::Convention).scope(scope);
            match self.space.scan(&pattern) {
                Ok(tuples) => {
                    for t in tuples {
                        if let Some(text) = t.payload.get("text").and_then(|v| v.as_str()) {
                            texts.push(text.to_string());
                        }
                    }
                }
                Err(e) => warn!(error = %e, scope, "failed to scan conventions for priming"),
            }
        }
        texts
    }

    fn emit_obstacle_for_budget(&self, record: &AgentRecord, kind: &str) {
        let tuple = Tuple::new(
            Category::Obstacle,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": format!("budget_{kind}"),
                "agent": record.name,
                "task": record.task,
                "cost_usd": record.cost_usd,
                "tokens": record.usage.total(),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit budget obstacle");
        }
    }

    /// Cost rollups behind the dispatch preflight, for one `repo` and (when
    /// given) one workflow `instance`.
    ///
    /// The **fleet/repo** tallies count only *live* (`Spawning`/`Running`)
    /// agents (`AgentState::is_live`): a record that has left the live fleet —
    /// `Completed`, `Failed`, `Dismissed`, or `Orphaned` — lingers in the
    /// registry (for respawn, `rk log`, history) but its spend drops off. That
    /// keeps `fleet_max_usd`/`repo_max_usd` standing guardrails on the *current
    /// live/concurrent* fleet, not cumulative lifetime ceilings that would
    /// refuse all spawns once lifetime spend crossed the cap. TKT-39 dropped
    /// only `Dismissed`, but steward-landed rats linger as `Completed` (the
    /// steward lands via a separate reviewer branch and never dismisses the
    /// original ticket-rat), so their spend still accumulated and could
    /// silently block continuous-drain and every other spawn (TKT-40).
    ///
    /// The **instance** tally is a different knob (TKT-32): a workflow's
    /// `budget:` caps the *cumulative* spend of one finite run, so a completed
    /// sequential step must still count against that run's total. It therefore
    /// counts every non-dismissed agent under the instance — an instance ends
    /// with the run, so lingering `Completed` spend can never block an
    /// unrelated future spawn the way it does for the fleet/repo caps.
    fn cost_rollup(&self, repo: &str, instance: Option<&str>) -> (f64, f64, f64) {
        let reg = self.lock_registry();
        let mut fleet = 0.0;
        let mut repo_total = 0.0;
        let mut instance_total = 0.0;
        // `list_all`, not `list`: archiving a record must never move a budget
        // number. It cannot change the fleet/repo tallies (only live agents
        // count, and archiving never touches a live record), but the instance
        // tally is cumulative over a run, so an archived Completed step still
        // has to count against its workflow's `budget:`.
        for a in reg.list_all() {
            if a.state.is_live() {
                fleet += a.cost_usd;
                if a.repo_name == repo {
                    repo_total += a.cost_usd;
                }
            }
            if instance.is_some()
                && a.workflow_instance.as_deref() == instance
                && a.state != AgentState::Dismissed
            {
                instance_total += a.cost_usd;
            }
        }
        (fleet, repo_total, instance_total)
    }

    /// Preflight fleet/repo/instance budget guard run before every spawn.
    /// Returns `Err` (refusing dispatch) once a cap is hit; posts an obstacle on
    /// both the warn band and the hard cap so it surfaces in `rk inbox`. When
    /// `instance`/`instance_cap` are set, the workflow's per-instance cap is
    /// enforced alongside the global fleet/repo caps.
    fn check_dispatch_budget(
        &self,
        repo: &str,
        instance: Option<&str>,
        instance_cap: Option<f64>,
    ) -> rk_core::Result<()> {
        let (fleet_spent, repo_spent, instance_spent) = self.cost_rollup(repo, instance);
        // Only fold in the instance scope when this spawn carries an instance id
        // and a positive cap; otherwise the check stays fleet/repo-only.
        let instance_arg = match (instance, instance_cap) {
            (Some(_), Some(cap)) if cap > 0.0 => Some((instance_spent, cap)),
            _ => None,
        };
        let check =
            self.fleet_budget
                .check_dispatch_scoped(fleet_spent, repo_spent, instance_arg);
        match check.action {
            BudgetAction::Ok => Ok(()),
            BudgetAction::Warn => {
                if let Some(scope) = check.scope {
                    if self.mark_fleet_warned(scope, repo, instance) {
                        warn!(scope = scope.as_str(), spent = check.spent_usd, cap = check.cap_usd, "budget warning threshold crossed");
                        self.emit_dispatch_obstacle(repo, scope, "warning", &check, instance);
                    }
                }
                Ok(())
            }
            BudgetAction::Stop => {
                let scope = check.scope.unwrap_or(BudgetScope::Fleet);
                warn!(scope = scope.as_str(), spent = check.spent_usd, cap = check.cap_usd, "budget cap hit — refusing dispatch");
                self.emit_dispatch_obstacle(repo, scope, "exceeded", &check, instance);
                Err(rk_core::Error::other(format!(
                    "{} budget cap hit: ${:.4} spent >= ${:.4} cap — dispatch refused",
                    scope.as_str(),
                    check.spent_usd,
                    check.cap_usd
                )))
            }
        }
    }

    /// Read-only preflight: would a spawn into `repo` be refused by the
    /// fleet/repo budget cap right now? Lets an autoscaler (the continuous-drain
    /// controller) skip claiming a ticket it could not dispatch, rather than
    /// claim-then-orphan it in `in_progress`. Side-effect free — unlike
    /// [`check_dispatch_budget`](Self::check_dispatch_budget), it emits no
    /// obstacle, so polling it every drain cycle does not spam `rk inbox`. The
    /// authoritative guard still runs inside `spawn`; this only avoids the claim.
    ///
    /// Preflight has no workflow instance in hand, so it checks only the
    /// fleet/repo scopes (`check_dispatch`); the per-instance cap (TKT-32) is
    /// still enforced by the authoritative guard in `spawn`.
    pub fn would_exceed_budget(&self, repo: &str) -> bool {
        let (fleet_spent, repo_spent, _instance_spent) = self.cost_rollup(repo, None);
        matches!(
            self.fleet_budget
                .check_dispatch(fleet_spent, repo_spent)
                .action,
            BudgetAction::Stop
        )
    }

    /// Returns true the first time a given scope is warned, so a warn obstacle
    /// is not re-posted on every subsequent dispatch in the band. The instance
    /// scope is keyed by instance id so each workflow run warns independently.
    fn mark_fleet_warned(&self, scope: BudgetScope, repo: &str, instance: Option<&str>) -> bool {
        let key = match scope {
            BudgetScope::Fleet => "__fleet__".to_string(),
            BudgetScope::Repo => format!("__repo__:{repo}"),
            BudgetScope::Instance => format!("__instance__:{}", instance.unwrap_or("")),
        };
        match self.fleet_warned.lock() {
            Ok(mut set) => set.insert(key),
            Err(p) => p.into_inner().insert(key),
        }
    }

    fn emit_dispatch_obstacle(
        &self,
        repo: &str,
        scope: BudgetScope,
        kind: &str,
        check: &DispatchCheck,
        instance: Option<&str>,
    ) {
        let mut payload = json!({
            "type": format!("budget_{}_{kind}", scope.as_str()),
            "scope": scope.as_str(),
            "spent_usd": check.spent_usd,
            "cap_usd": check.cap_usd,
        });
        // Name the offending instance on an instance-scoped obstacle so the
        // operator can tell which workflow run hit its cap.
        if scope == BudgetScope::Instance {
            if let Some(id) = instance {
                payload["instance"] = json!(id);
            }
        }
        let tuple = Tuple::new(
            Category::Obstacle,
            repo.to_string(),
            format!("budget-{}", scope.as_str()),
            self.castle.clone(),
            payload,
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit dispatch budget obstacle");
        }
    }

    /// Fleet + per-repo cost rollup against the configured caps, for
    /// `rk cost --fleet`. Read-only; mirrors the denominator `check_dispatch`
    /// enforces on, matching `cost_rollup`: the fleet/repo totals count only
    /// live (`Spawning`/`Running`) agents — completed/failed/dismissed/orphaned
    /// records drop off — while per-instance spend stays cumulative (every
    /// non-dismissed agent under the instance), since a workflow's `budget:` is
    /// a lifetime cap on one finite run (TKT-40).
    pub fn fleet_rollup(&self) -> serde_json::Value {
        use std::collections::BTreeMap;
        let mut fleet_spent = 0.0;
        let mut per_repo: BTreeMap<String, f64> = BTreeMap::new();
        let mut per_instance: BTreeMap<String, f64> = BTreeMap::new();
        {
            let reg = self.lock_registry();
            // Full history (live + archived), matching `cost_rollup`: archiving
            // is a UI operation, never a budget one.
            for a in reg.list_all() {
                if a.state.is_live() {
                    fleet_spent += a.cost_usd;
                    *per_repo.entry(a.repo_name.clone()).or_default() += a.cost_usd;
                }
                // Per-instance spend is cumulative over the run (TKT-32): count
                // every non-dismissed agent, including completed sequential
                // steps, so it mirrors the instance-cap denominator.
                if a.state != AgentState::Dismissed {
                    if let Some(inst) = &a.workflow_instance {
                        *per_instance.entry(inst.clone()).or_default() += a.cost_usd;
                    }
                }
            }
        }
        let fb = &self.fleet_budget;
        let repos: Vec<serde_json::Value> = per_repo
            .into_iter()
            .map(|(repo, spent)| scope_json(spent, fb.repo_max_usd, fb.warn_at, Some(repo)))
            .collect();
        // Per-instance spend is reported cap-less: an instance's cap lives on
        // its workflow definition, not in the fleet config, so this rollup shows
        // current burn per running workflow (the cap is enforced at dispatch).
        let instances: Vec<serde_json::Value> = per_instance
            .into_iter()
            .map(|(instance, spent)| json!({"instance": instance, "spent_usd": spent}))
            .collect();
        json!({
            "fleet": scope_json(fleet_spent, fb.fleet_max_usd, fb.warn_at, None),
            "repos": repos,
            "instances": instances,
        })
    }

    /// One liveness/burn-rate sweep over the live, headless (event-pumped) rats.
    ///
    /// Budget checks fire only on Usage events, so a rat hung mid-tool-call
    /// emitting nothing never trips them. This out-of-band pass compares each
    /// rat's `updated_at` (bumped on every event via `Registry::update`) to now
    /// — silence past `stuck_after_secs` is STUCK — and tracks cost across
    /// sweeps — sustained USD/min above `burn_usd_per_min` is RUNNING AWAY.
    ///
    /// The response is graduated and mirrors budget enforcement: the first sweep
    /// to flag an agent posts an obstacle tuple and soft-steers it ("still
    /// working? wrap up"); only if it is STILL flagged after `kill_grace_secs`
    /// does the next sweep escalate to a kill. A steer that revives the rat (any
    /// new event bumps `updated_at`) clears the flag before it can be killed.
    ///
    /// Attach-mode rats and any without a live control handle are skipped: their
    /// liveness isn't tracked through the event pump, so silence proves nothing.
    pub fn sweep(&self, cfg: &SupervisorConfig) {
        let now = Utc::now();
        let live: Vec<AgentRecord> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| r.state.is_live())
            .cloned()
            .collect();
        let live_names: std::collections::HashSet<&str> =
            live.iter().map(|r| r.name.as_str()).collect();

        for record in &live {
            // Only headless rats we actively control are event-pumped, so only
            // for them does `updated_at` silence mean anything — and only them
            // can we steer/kill through this path.
            if !self.lock_controls().contains_key(&record.name) {
                continue;
            }
            let action = self.decide_sweep(record, now, cfg);
            match action {
                SweepAction::None => {}
                SweepAction::Soft { kind, detail } => {
                    warn!(agent = %record.name, kind, %detail, "supervisor sweep flagged agent");
                    self.emit_sweep_obstacle(record, kind, &detail);
                    self.steer_flagged(record, kind);
                }
                SweepAction::Hard { kind, detail } => {
                    warn!(agent = %record.name, kind, %detail, "supervisor sweep killing agent after grace");
                    self.emit_sweep_obstacle(record, kind, &format!("{detail} — killed after grace"));
                    let control = self.lock_controls().remove(&record.name);
                    if let Some(control) = control {
                        tokio::spawn(async move {
                            let _ = control.kill().await;
                        });
                    }
                }
            }
        }

        // Drop bookkeeping for agents that are no longer live so a later respawn
        // starts from a clean episode.
        self.lock_sweep_state()
            .retain(|name, _| live_names.contains(name.as_str()));
    }

    /// Update this agent's rolling sweep state and decide what to do about it.
    /// All state mutation happens here under the one lock; the caller acts on
    /// the returned decision after the lock is released.
    fn decide_sweep(
        &self,
        record: &AgentRecord,
        now: DateTime<Utc>,
        cfg: &SupervisorConfig,
    ) -> SweepAction {
        let mut state = self.lock_sweep_state();
        let st = state.entry(record.name.clone()).or_insert(SweepState {
            last_cost_usd: record.cost_usd,
            last_observed: now,
            flagged_at: None,
        });

        // Burn rate (USD/min) since the previous sweep of this agent.
        let dt_min = (now - st.last_observed).num_milliseconds() as f64 / 60_000.0;
        let burn = if dt_min > 0.0 {
            (record.cost_usd - st.last_cost_usd) / dt_min
        } else {
            0.0
        };
        st.last_cost_usd = record.cost_usd;
        st.last_observed = now;

        let idle_secs = (now - record.updated_at).num_seconds().max(0) as u64;
        let stuck = cfg.stuck_after_secs > 0 && idle_secs >= cfg.stuck_after_secs;
        let running_away = cfg.burn_usd_per_min > 0.0 && burn >= cfg.burn_usd_per_min;

        if !stuck && !running_away {
            // Recovered (or never flagged): clear any open episode.
            st.flagged_at = None;
            return SweepAction::None;
        }

        // Stuck takes precedence in the message; both post an obstacle whose
        // `type` a reactor #Trigger can match ("stuck" / "runaway").
        let (kind, detail): (&'static str, String) = if stuck {
            ("stuck", format!("no events for {idle_secs}s while still running"))
        } else {
            ("runaway", format!("sustained burn ${burn:.2}/min with no completion"))
        };

        match st.flagged_at {
            None => {
                st.flagged_at = Some(now);
                SweepAction::Soft { kind, detail }
            }
            Some(flagged) => {
                let elapsed = (now - flagged).num_seconds().max(0) as u64;
                if elapsed >= cfg.kill_grace_secs {
                    SweepAction::Hard { kind, detail }
                } else {
                    SweepAction::None
                }
            }
        }
    }

    fn steer_flagged(&self, record: &AgentRecord, kind: &str) {
        let control = self.lock_controls().get(&record.name).cloned();
        if let Some(control) = control {
            let name = record.name.clone();
            let nudge = if kind == "stuck" {
                format!(
                    "SUPERVISOR CHECK for {name}: you have gone quiet — still working? \
                     If you are stuck, record it with `rk obstacle`, then either make \
                     progress or wrap up: commit what you have and run `rk done` now."
                )
            } else {
                format!(
                    "SUPERVISOR CHECK for {name}: you are burning cost fast with no \
                     completion. Wrap up: commit what you have and run `rk done` now."
                )
            };
            tokio::spawn(async move {
                let _ = control.steer(&nudge).await;
            });
        }
    }

    fn emit_sweep_obstacle(&self, record: &AgentRecord, kind: &str, detail: &str) {
        let tuple = Tuple::new(
            Category::Obstacle,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": kind,
                "agent": record.name,
                "task": record.task,
                "detail": detail,
                "cost_usd": record.cost_usd,
                "tokens": record.usage.total(),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit sweep obstacle");
        }
    }

    /// Self-healing respawn sweep: auto-`respawn` agents that crashed out of
    /// their run — `Orphaned` (a daemon restart killed the process, worktree
    /// preserved) or `Failed` (the harness died non-zero) — so a transient
    /// crash stops being a manual `rk respawn` chore.
    ///
    /// Bounded by a crash-loop backoff so a genuinely-broken task cannot
    /// respawn-loop forever: each agent is respawned up to
    /// `respawn_max_attempts` times, the retries spaced by exponential backoff
    /// (`respawn_backoff_secs * 2^(attempt-1)`); once the cap is hit the sweep
    /// escalates a `need` (surfaced by `rk inbox`) for a human and stops.
    ///
    /// Guardrail: an agent whose branch already merged is never respawned — its
    /// work already landed, so a respawn would redo merged work. It is dropped
    /// from tracking instead.
    ///
    /// Shares the liveness-sweep loop (TKT-15): the server calls this right
    /// after `sweep()` on the same tick.
    pub fn respawn_sweep(self: &Arc<Self>, cfg: &SupervisorConfig) {
        if !cfg.respawn_enabled || cfg.respawn_max_attempts == 0 {
            return;
        }
        let now = Utc::now();
        // Candidates: crashed but not dismissed and not cleanly completed. A
        // `Completed` rat ran `rk done` — a clean finish we must not relaunch.
        let candidates: Vec<AgentRecord> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| matches!(r.state, AgentState::Orphaned | AgentState::Failed))
            .cloned()
            .collect();

        for record in &candidates {
            // Guardrail: never auto-respawn an agent whose branch already merged
            // (or was deleted) — its work is done; a respawn would redo it.
            if self.branch_already_merged(record) {
                self.lock_respawn_state().remove(&record.name);
                continue;
            }
            match self.decide_respawn(record, now, cfg) {
                RespawnDecision::Wait => {}
                RespawnDecision::Respawn => {
                    // Respect the wallet: an operator who set a fleet/repo cap
                    // does not want auto-respawn to blow past it. Skip this
                    // cycle without counting the attempt if we're over the cap.
                    if self.would_exceed_budget(&record.repo_name) {
                        warn!(agent = %record.name, "skipping auto-respawn: over budget cap");
                        continue;
                    }
                    let attempt = self.record_respawn_attempt(&record.name, now);
                    info!(
                        agent = %record.name,
                        attempt,
                        max = cfg.respawn_max_attempts,
                        "self-healing sweep respawning crashed agent"
                    );
                    if let Err(e) = self.respawn(&record.name) {
                        warn!(agent = %record.name, error = %e, "auto-respawn failed");
                    }
                }
                RespawnDecision::Escalate => {
                    self.escalate_respawn_cap(record, cfg);
                }
            }
        }

        // Forget bookkeeping for agents that reached a terminal-clean state
        // (Completed/Dismissed) or vanished; a Running/Failed/Orphaned agent
        // keeps its counter so the Running->Failed cycle stays bounded.
        let keep: std::collections::HashSet<String> = self
            .lock_registry()
            .list()
            .into_iter()
            .filter(|r| !matches!(r.state, AgentState::Completed | AgentState::Dismissed))
            .map(|r| r.name.clone())
            .collect();
        self.lock_respawn_state()
            .retain(|name, _| keep.contains(name.as_str()));
    }

    /// Decide what to do about one crashed agent. Reads (does not mutate) the
    /// respawn-state so the caller can act — the attempt is recorded separately
    /// via `record_respawn_attempt` only if the respawn actually launches.
    fn decide_respawn(
        &self,
        record: &AgentRecord,
        now: DateTime<Utc>,
        cfg: &SupervisorConfig,
    ) -> RespawnDecision {
        let state = self.lock_respawn_state();
        let st = state.get(&record.name);
        let attempts = st.map(|s| s.attempts).unwrap_or(0);

        if attempts >= cfg.respawn_max_attempts {
            // Cap hit: escalate once, then stay quiet until a human intervenes.
            return if st.map(|s| s.escalated).unwrap_or(false) {
                RespawnDecision::Wait
            } else {
                RespawnDecision::Escalate
            };
        }

        // First attempt fires immediately; every retry waits out an exponential
        // backoff measured from the previous attempt.
        match st {
            None => RespawnDecision::Respawn,
            Some(st) => {
                let backoff = cfg
                    .respawn_backoff_secs
                    .saturating_mul(1u64 << (attempts.saturating_sub(1)).min(16));
                let waited = (now - st.last_attempt).num_seconds().max(0) as u64;
                if waited >= backoff {
                    RespawnDecision::Respawn
                } else {
                    RespawnDecision::Wait
                }
            }
        }
    }

    /// Record that an auto-respawn just fired for `name`, bumping its attempt
    /// count and resetting the backoff clock. Returns the new attempt number.
    fn record_respawn_attempt(&self, name: &str, now: DateTime<Utc>) -> u32 {
        let mut state = self.lock_respawn_state();
        let st = state.entry(name.to_string()).or_insert(RespawnState {
            attempts: 0,
            last_attempt: now,
            escalated: false,
        });
        st.attempts += 1;
        st.last_attempt = now;
        st.attempts
    }

    /// Whether the self-healing respawn sweep has already given up on `name`:
    /// its crash-loop cap was hit and escalated to a human, so no further
    /// auto-respawn will fire and the agent will not come back on its own.
    ///
    /// Read by the workflow engine (TKT-147) to tell a crashed rat that may yet
    /// be revived from one that is gone for good — a `wait` must keep blocking
    /// for the former and must fail fast on the latter.
    pub fn respawn_exhausted(&self, name: &str) -> bool {
        self.lock_respawn_state()
            .get(name)
            .map(|st| st.escalated)
            .unwrap_or(false)
    }

    /// The merged-branch guardrail: true if this agent's work already landed
    /// (so a respawn would redo merged work) or its branch is gone (nothing to
    /// resume). "Merged" here is precise — the branch is *strictly behind* its
    /// target: contained in it yet the target has advanced past it. That
    /// deliberately excludes a branch that merely equals its target (an agent
    /// that crashed before committing anything: tip == base), which is exactly
    /// the transient crash we most want to auto-respawn — a plain
    /// "is-ancestor" test would mis-skip it. An unmerged branch (commits not in
    /// target) is not an ancestor, so it respawns. Fail-safe: an unresolvable
    /// repo reads as "not merged" so we never wrongly skip a recoverable agent.
    fn branch_already_merged(&self, record: &AgentRecord) -> bool {
        let Some(branch) = record.branch.as_deref() else {
            return false;
        };
        let Ok(repo) = Repo::discover(&record.repo_root) else {
            return false;
        };
        if !repo.branch_exists(branch) {
            return true; // gone: the worktree can't be resumed onto it.
        }
        let target = &record.target_branch;
        repo.is_ancestor(branch, target) && !repo.is_ancestor(target, branch)
    }

    /// Escalate an exhausted crash-loop to a human: emit a `need` tuple (which
    /// `rk inbox` surfaces) and mark the agent escalated so we do it only once.
    fn escalate_respawn_cap(&self, record: &AgentRecord, cfg: &SupervisorConfig) {
        {
            let mut state = self.lock_respawn_state();
            if let Some(st) = state.get_mut(&record.name) {
                st.escalated = true;
            }
        }
        warn!(
            agent = %record.name,
            attempts = cfg.respawn_max_attempts,
            "auto-respawn cap exhausted — escalating a need for a human"
        );
        let tuple = Tuple::new(
            Category::Need,
            record.repo_name.clone(),
            record.name.clone(),
            self.castle.clone(),
            json!({
                "type": "respawn_exhausted",
                "agent": record.name,
                "task": record.task,
                "attempts": cfg.respawn_max_attempts,
                "text": format!(
                    "agent {} crashed and exhausted {} auto-respawn attempts; \
                     needs a human — investigate then `rk respawn {}`",
                    record.name, cfg.respawn_max_attempts, record.name
                ),
            }),
        );
        if let Err(e) = self.space.out(tuple.into_trail(DEFAULT_TRAIL_TTL)) {
            warn!(error = %e, "failed to emit respawn-exhausted need");
        }
    }

    /// Decide whether the turn that just ended is the one this generation
    /// finishes on, and claim the right to publish it.
    /// [`TurnClaim::publish`] = publish now.
    ///
    /// A harness returns control once per TURN. A background test suite
    /// finishing, a re-armed monitor, or a task notification each end a turn
    /// mid-task, and the `Completed` event they produce is indistinguishable
    /// from the real thing: `is_error` is `false` on both, and no other field
    /// says "still working". Publishing every one of them is TKT-160 — twelve
    /// agent generations in the live fleet emitted more than one durable
    /// `harness_result`, and because a `LIMIT 1` read returns the OLDEST match,
    /// every reader keyed on the agent name gets a MID-FLIGHT turn: a workflow
    /// `wait` unblocks on "the full cargo test pass is still running", the
    /// `evaluate` behind it judges that text, and a steward reviewer whose
    /// APPROVE lands in a later turn is read as having no verdict at all.
    ///
    /// Two things prove a turn is the last one, and this is where the first is
    /// applied:
    ///
    /// 1. **The agent said so.** `rk done` writes exactly one `task_done` per
    ///    generation — the one signal in the system that a harness cannot
    ///    duplicate, because the rat writes it rather than the harness. Every
    ///    spawned role is primed with `rk done` as its mandatory final step.
    /// 2. **The process is gone.** Handled at `Exited`; see
    ///    [`Self::flush_withheld_completion`].
    ///
    /// A failed turn is terminal on its own: the session ended in an error, so
    /// there is no later turn to prefer, and holding it back would only turn a
    /// fast, legible failure into a `wait` timeout.
    fn claim_completion(&self, name: &str, generation: DateTime<Utc>, is_error: bool) -> TurnClaim {
        // Asked unconditionally rather than short-circuited behind `is_error`,
        // because the answer is published as `declared_done` and a failed turn
        // has one too: a rat can run `rk done` and then have a later turn error
        // out. Costs one indexed scan on a path that runs once per turn.
        let declared_done = self.declared_done(name, generation);
        let terminal = is_error || declared_done;
        let mut completions = self.lock_completions();
        let state = completions
            .entry(name.to_string())
            .or_insert_with(|| CompletionState {
                generation,
                routed: false,
                withheld: false,
            });
        if state.generation != generation {
            *state = CompletionState {
                generation,
                routed: false,
                withheld: false,
            };
        }
        if state.routed {
            // Already published for this generation. A rat that keeps talking
            // after its `rk done` does not get to report twice.
            return TurnClaim {
                publish: false,
                declared_done,
            };
        }
        if terminal {
            state.routed = true;
            state.withheld = false;
        } else {
            // Superseded by whatever turn comes next, or flushed at `Exited`.
            state.withheld = true;
        }
        TurnClaim {
            publish: terminal,
            declared_done,
        }
    }

    /// Whether this generation of `name` has written its `rk done` tuple.
    ///
    /// Bounded to the generation via [`Pattern::for_agent_since`]: `task_done`
    /// is durable and a name is an identity key that outlives the rat wearing
    /// it, so an unbounded name search here would read a predecessor's `rk done`
    /// as this rat's (TKT-146/TKT-159).
    ///
    /// Fails OPEN — an unreadable space means "publish", which is the behaviour
    /// that predates this gate. Withholding on a storage error would strand
    /// every workflow waiting on the agent until its step timeout.
    fn declared_done(&self, name: &str, generation: DateTime<Utc>) -> bool {
        let pattern = Pattern::for_agent_since(Category::Event, "task_done", name, generation);
        match self.space.scan(&pattern) {
            Ok(tuples) => !tuples.is_empty(),
            Err(e) => {
                warn!(error = %e, agent = name, "task_done lookup failed; publishing the turn result anyway");
                true
            }
        }
    }

    /// Claim the right to publish a turn result that was held back, now that the
    /// process has exited and no further turn can follow. `true` = publish.
    ///
    /// A `true` here is also the proof that the generation never declared itself
    /// done — withholding is the only way a result reaches this path — which is
    /// why the caller publishes it with `declared_done: false` and, since
    /// TKT-175, as `is_error: true` however the process ended.
    fn flush_withheld_completion(&self, name: &str, generation: DateTime<Utc>) -> bool {
        let mut completions = self.lock_completions();
        let Some(state) = completions.get_mut(name) else {
            return false;
        };
        if state.generation != generation || state.routed || !state.withheld {
            return false;
        }
        state.routed = true;
        state.withheld = false;
        true
    }

    /// Forget a generation's completion bookkeeping.
    ///
    /// Called on a deliberate teardown (`dismiss`), where the held-back turn
    /// result must NOT surface as a late completion — nothing is waiting on it,
    /// and a stray `harness_result` carrying `"role":"rat"` would re-fire the
    /// reactor's steward on a branch that was just merged. Also called on
    /// `respawn`, which continues the SAME generation in a fresh process: the
    /// crashed run's withheld turn is stale, and its `routed` flag would
    /// otherwise gag the resumed run.
    fn forget_completion(&self, name: &str) {
        self.lock_completions().remove(name);
    }

    /// Route a completion up the spawn tree: the structural parent gets a
    /// directed message; the repo scope gets the event either way.
    ///
    /// Reached exactly once per agent generation — see
    /// [`Self::claim_completion`] for what "once" means and why it matters.
    fn route_completion(&self, record: &AgentRecord, is_error: bool, declared_done: bool) {
        self.emit_event(
            &record.repo_name,
            "harness_result",
            json!({
                "agent": record.name,
                // The completed agent's role ("rat", "reviewer", ...). Carried so
                // a reactor trigger can scope reactively — e.g. the steward fires
                // on `"role":"rat"` completions only, which also breaks its own
                // re-entrancy: the reviewer it spawns completes as "reviewer" and
                // never re-triggers the steward on the branch it just reviewed.
                "role": record.role,
                "task": record.task,
                "branch": record.branch,
                "parent": record.parent,
                "is_error": is_error,
                // Whether the agent itself declared the task finished (`rk done`)
                // in this generation, as opposed to merely stopping — killed by
                // the budget hard-stop, swept, or exiting mid-task (TKT-173).
                // Since TKT-175 every undeclared generation is also
                // `is_error: true`, so this no longer carries a fact `is_error`
                // lacks on the exit path. It still discriminates on the OTHER
                // path: a turn that errored out is undeclared too, so a reader
                // that wants "the rat said it was done" — rather than "nothing
                // went wrong" — reads this instead of inferring it from prose.
                "declared_done": declared_done,
                "result": record.result,
                "cost_usd": record.cost_usd,
                "tokens": record.usage.total(),
            }),
        );
        if let Some(parent) = &record.parent {
            let tuple = Tuple::new(
                Category::Message,
                record.repo_name.clone(),
                parent.clone(),
                self.castle.clone(),
                json!({
                    "type": "child_completed",
                    "child": record.name,
                    "task": record.task,
                    "is_error": is_error,
                    "result": record.result,
                }),
            );
            if let Err(e) = self.space.out(tuple) {
                warn!(error = %e, "failed to notify parent");
            }
        }
        // A rat dispatched from a ticket closes that ticket's loop: a clean
        // finish marks it done (which unblocks any dependents), an error leaves
        // it in_progress for inspection. Fire-and-forget so completion routing
        // is never held up by the ticket's serialization lock.
        //
        // Since TKT-173 a rat KILLED mid-task reports an error, so it no longer
        // closes the ticket it never finished — and no dependent is unblocked
        // behind it. A rat that exits without `rk done` still closes its ticket;
        // TKT-175 is where that gets revisited.
        if !is_error {
            if let Some(task) = record.task.clone() {
                if task.starts_with(crate::tickets::ID_PREFIX) {
                    let tickets = self.tickets.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tickets.set_status(&task, "done").await {
                            warn!(ticket = %task, error = %e, "failed to mark ticket done");
                        }
                    });
                }
            }
        }
    }

    pub async fn steer(&self, name: &str, message: &str) -> rk_core::Result<()> {
        let control = self.lock_controls().get(name).cloned();
        if let Some(control) = control {
            return control.steer(message).await;
        }
        // Attach-mode rats steer through their herdr pane.
        let target = self
            .lock_registry()
            .get(name)
            .and_then(|r| r.attach_target.clone());
        if let Some(target) = target {
            let message = message.to_string();
            return tokio::task::spawn_blocking(move || rk_mux::HerdrMux::send(&target, &message))
                .await
                .map_err(|e| rk_core::Error::other(e.to_string()))?;
        }
        Err(rk_core::Error::other(format!("{name} has no live session")))
    }

    pub async fn interrupt(&self, name: &str) -> rk_core::Result<()> {
        let control = self
            .lock_controls()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("{name} has no live session")))?;
        control.interrupt().await
    }

    /// Resolve a repo's merge policy — how a landed branch reaches its base.
    /// Reads the on-disk repo registry (`repos.json`), which the daemon
    /// persists synchronously on every `repo add`, so this sees the current
    /// policy without sharing mutable state with the server. Returns the
    /// repo's `(merge_mode, remote, host)`; an unregistered repo (or an
    /// unreadable registry) resolves to the pre-PR-mode default — a direct
    /// merge into `origin`.
    fn merge_policy(&self, repo_name: &str) -> (MergeMode, String, Option<String>) {
        let path = self.layout.home().join("repos.json");
        match crate::repos::RepoRegistry::load(&path) {
            Ok(reg) => match reg.get(repo_name) {
                Some(rec) => (
                    rec.merge_mode,
                    rec.remote_or_default().to_string(),
                    rec.host.clone(),
                ),
                None => (MergeMode::Direct, "origin".to_string(), None),
            },
            Err(_) => (MergeMode::Direct, "origin".to_string(), None),
        }
    }

    /// Dismiss: stop the session if live, then reconcile the branch with its
    /// target per the repo's merge mode — a `Direct` merge into the target (and
    /// branch delete on success), or a `Pr` push + opened pull/merge request
    /// that leaves the branch standing for review. Always removes the worktree.
    pub async fn dismiss(&self, name: &str, no_merge: bool) -> rk_core::Result<serde_json::Value> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;

        // Drop any held-back turn result BEFORE the kill: the `Exited` this
        // provokes must not publish a late `harness_result` for an agent the
        // caller is deliberately tearing down (TKT-160).
        self.forget_completion(name);
        let control = self.lock_controls().remove(name);
        if let Some(control) = control {
            let _ = control.kill().await;
            // Give the child a moment to exit cleanly before touching git.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
        if let Some(target) = record.attach_target.clone() {
            let _ = tokio::task::spawn_blocking(move || rk_mux::HerdrMux::close(&target)).await;
        }

        let repo = Repo::discover(&record.repo_root)?;
        let mut merged = false;
        let mut merge_commit: Option<String> = None;
        let mut pr_opened = false;
        let mut pr_url: Option<String> = None;
        let mut detail = String::from("no merge requested");
        let (merge_mode, remote, _host) = self.merge_policy(&record.repo_name);

        if let Some(worktree) = &record.worktree {
            if worktree.exists() {
                repo.remove_worktree(worktree)?;
            }
        }
        if let Some(branch) = &record.branch {
            if no_merge {
                detail = format!("branch {branch} preserved (--no-merge)");
            } else {
                match merge_mode {
                    MergeMode::Direct => {
                        // Take our turn in the per-target merge queue: only one
                        // land or dismiss into this target runs at a time, so
                        // this merge sees a target no concurrent auto-merge is
                        // moving underneath it. Held only across the merge
                        // itself — the kill/worktree cleanup above and the
                        // branch delete below stay parallel across a fan-out.
                        let outcome = {
                            let _merge_guard = self
                                .merge_queue
                                .acquire(repo.root(), &record.target_branch)
                                .await;
                            repo.merge_branch(branch, &record.target_branch)?
                        };
                        merged = outcome.merged;
                        merge_commit = outcome.commit;
                        detail = outcome.detail;
                        if merged {
                            repo.delete_branch(branch)?;
                        }
                    }
                    MergeMode::Pr => {
                        // PR mode never merges or deletes the branch: it pushes
                        // and opens a pull/merge request, leaving the branch
                        // standing for a human to review and merge. A push/auth
                        // failure is a clean `pr_opened: false` (never an error),
                        // mirroring the merge path's `merged: false`.
                        let outcome =
                            repo.open_pull_request(branch, &record.target_branch, &remote);
                        pr_opened = outcome.opened;
                        pr_url = outcome.url;
                        detail = outcome.detail;
                    }
                }
            }
        }

        self.lock_registry().update(name, |r| {
            r.state = AgentState::Dismissed;
            r.pid = None;
            // Record the landed merge commit as the `rk revert` anchor; a
            // no-merge or PR-mode dismiss leaves any prior record untouched.
            if merge_commit.is_some() {
                r.merge_commit = merge_commit.clone();
            }
        })?;
        // A merged ticket-rat closes its ticket for good.
        if merged {
            if let Some(task) = &record.task {
                if task.starts_with(crate::tickets::ID_PREFIX) {
                    if let Err(e) = self.tickets.set_status(task, "closed").await {
                        warn!(ticket = %task, error = %e, "failed to close ticket on dismiss");
                    }
                }
            }
        }
        self.emit_event(
            &record.repo_name,
            "agent_dismissed",
            json!({
                "agent": name,
                "merged": merged,
                "merge_commit": &merge_commit,
                "pr_opened": pr_opened,
                "pr_url": &pr_url,
                "detail": &detail,
                "parent": &record.parent,
            }),
        );
        // A PR-mode dismiss hands the branch off for review rather than merging;
        // surface that as its own event so the inbox / steward can pick it up.
        if pr_opened {
            self.emit_event(
                &record.repo_name,
                "pull_request_opened",
                json!({
                    "agent": name,
                    "branch": &record.branch,
                    "target": &record.target_branch,
                    "url": &pr_url,
                    "detail": &detail,
                    "parent": &record.parent,
                }),
            );
        }
        Ok(json!({
            "agent": name,
            "merged": merged,
            "merge_commit": merge_commit,
            "pr_opened": pr_opened,
            "pr_url": pr_url,
            "detail": detail,
        }))
    }

    /// Revert a dismissed agent's landed merge — the undo for an unattended
    /// auto-merge that turned out bad (steward/drain landed it, then main
    /// broke). Revert-merges the merge commit recorded on the agent's record
    /// at dismiss time (CAS-safe, through the same per-target merge queue as
    /// land/dismiss), reopens the agent's ticket (`open`, or `blocked` with
    /// `block` to hold it out of the auto-dispatch backlog), and emits a
    /// `fact` tuple recording what was undone. A revert conflict or a target
    /// moved mid-revert is a clean `reverted: false`, mirroring merge; an
    /// agent that never merged (no-merge, PR-mode, or already reverted) is an
    /// error.
    pub async fn revert(&self, name: &str, block: bool) -> rk_core::Result<serde_json::Value> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;
        let Some(commit) = record.merge_commit.clone() else {
            return Err(rk_core::Error::other(format!(
                "{name} has no recorded merge commit to revert \
                 (never merged, PR-mode, or already reverted)"
            )));
        };

        let repo = Repo::discover(&record.repo_root)?;
        // Same per-target merge queue as land/dismiss: the revert takes its
        // turn so it never races a concurrent auto-merge into this target.
        let outcome = {
            let _merge_guard = self
                .merge_queue
                .acquire(repo.root(), &record.target_branch)
                .await;
            repo.revert_merge(&commit, &record.target_branch)?
        };
        let reverted = outcome.merged;

        let mut ticket_status: Option<&str> = None;
        if reverted {
            // Clear the anchor so a second `rk revert` errors instead of
            // reverting the revert.
            self.lock_registry().update(name, |r| {
                r.merge_commit = None;
            })?;
            // Reopen the ticket the bad merge closed, so the work is durably
            // back on the backlog rather than falsely done.
            if let Some(task) = &record.task {
                if task.starts_with(crate::tickets::ID_PREFIX) {
                    let status = if block { "blocked" } else { "open" };
                    match self.tickets.set_status(task, status).await {
                        Ok(_) => ticket_status = Some(status),
                        Err(e) => {
                            warn!(ticket = %task, error = %e, "failed to reopen ticket on revert");
                        }
                    }
                }
            }
            let fact = Tuple::new(
                Category::Fact,
                record.repo_name.clone(),
                format!("merge-reverted-{name}"),
                self.castle.clone(),
                json!({
                    "agent": name,
                    "branch": &record.branch,
                    "target": &record.target_branch,
                    "task": &record.task,
                    "merge_commit": &commit,
                    "revert_commit": &outcome.commit,
                    "ticket_status": ticket_status,
                    "detail": &outcome.detail,
                }),
            );
            if let Err(e) = self.space.out(fact) {
                warn!(error = %e, "failed to emit merge-reverted fact tuple");
            }
        }
        info!(agent = name, reverted, merge_commit = %commit, "revert");
        Ok(json!({
            "agent": name,
            "reverted": reverted,
            "merge_commit": commit,
            "revert_commit": outcome.commit,
            "target": record.target_branch,
            "task": record.task,
            "ticket_status": ticket_status,
            "detail": outcome.detail,
        }))
    }

    /// Land a NAMED branch into a target — the explicit `{branch, target}`
    /// counterpart to [`dismiss`](Self::dismiss), which reconciles an agent's
    /// branch with its own base. Names neither an agent nor a worktree.
    ///
    /// Routes on the repo's merge mode, exactly like `dismiss`:
    /// - **Direct** — merge through a detached worktree (CAS-safe, touching no
    ///   live checkout). A merge conflict or a target that moved mid-merge is a
    ///   clean `merged: false`, not an error, so a workflow can gate on the
    ///   result (`evaluate {expect: {merged: true}}`) and retry rather than
    ///   fail. On success the source branch is deleted unless `keep_branch`;
    ///   deletion is best-effort (a protected or still-checked-out branch is
    ///   left in place and reported `branch_deleted: false`) so it never masks
    ///   the merge that already succeeded.
    /// - **Pr** — push the branch and open a pull/merge request against the
    ///   target, leaving the branch standing (`keep_branch` is implied) and
    ///   `merged: false`. A push failure is a clean `pr_opened: false`.
    pub async fn land(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        target: &str,
        keep_branch: bool,
    ) -> rk_core::Result<serde_json::Value> {
        let repo = Repo::discover(repo_root)?;
        let (merge_mode, remote, _host) = self.merge_policy(&repo.name());
        let mut merged = false;
        let mut branch_deleted = false;
        let mut pr_opened = false;
        let mut pr_url: Option<String> = None;
        let detail;
        match merge_mode {
            MergeMode::Direct => {
                // Same land / merge queue the agent-dismiss path uses: serialize
                // with any concurrent land/dismiss into this target so the merge
                // runs on the freshly-updated target rather than racing another
                // auto-merge (TKT-51).
                let outcome = {
                    let _merge_guard = self.merge_queue.acquire(repo.root(), target).await;
                    repo.merge_branch(branch, target)?
                };
                merged = outcome.merged;
                detail = outcome.detail;
                if merged && !keep_branch {
                    match repo.delete_branch(branch) {
                        Ok(()) => branch_deleted = true,
                        Err(e) => warn!(
                            branch,
                            error = %e,
                            "land: merged but could not delete source branch"
                        ),
                    }
                }
            }
            MergeMode::Pr => {
                // PR mode never merges or deletes the branch: push and open the
                // pull/merge request, leaving it for review.
                let outcome = repo.open_pull_request(branch, target, &remote);
                pr_opened = outcome.opened;
                pr_url = outcome.url;
                detail = outcome.detail;
            }
        }
        let result = json!({
            "branch": branch,
            "target": target,
            "merged": merged,
            "pr_opened": pr_opened,
            "pr_url": pr_url,
            "detail": detail,
            "branch_deleted": branch_deleted,
        });
        self.emit_event(&repo.name(), "branch_landed", result.clone());
        // Surface an opened PR as its own event, mirroring dismiss.
        if pr_opened {
            self.emit_event(
                &repo.name(),
                "pull_request_opened",
                json!({
                    "branch": branch,
                    "target": target,
                    "url": result.get("pr_url"),
                    "detail": result.get("detail"),
                }),
            );
        }
        info!(
            branch,
            target,
            merged,
            pr_opened,
            branch_deleted,
            "land"
        );
        Ok(result)
    }

    /// Open a pull/merge request for a NAMED branch against a target — the PR
    /// counterpart to [`land`](Self::land). Where `land` routes on the repo's
    /// merge mode (so it only opens a PR when the repo is registered PR-mode),
    /// `open_pr` ALWAYS pushes the branch and opens a pull/merge request,
    /// regardless of policy. This lets a workflow choose the review-by-PR
    /// outcome explicitly (e.g. `pr-on-approve.cue`) even in a Direct-merge repo.
    ///
    /// The branch is never merged or deleted; it is pushed and left standing for
    /// review. A push/auth failure is a clean `pr_opened: false` (never an
    /// error), mirroring `land`'s `merged: false`, so a workflow can gate on the
    /// result rather than fail. The remote comes from the repo's registered
    /// policy (defaulting to `origin`); only the merge *mode* is ignored.
    pub async fn open_pr(
        &self,
        repo_root: &std::path::Path,
        branch: &str,
        target: &str,
    ) -> rk_core::Result<serde_json::Value> {
        let repo = Repo::discover(repo_root)?;
        let (_merge_mode, remote, _host) = self.merge_policy(&repo.name());
        let outcome = repo.open_pull_request(branch, target, &remote);
        let result = json!({
            "branch": branch,
            "target": target,
            "merged": false,
            "pr_opened": outcome.opened,
            "pr_url": outcome.url,
            "detail": outcome.detail,
        });
        // Surface an opened PR as its own event, exactly as `land`/`dismiss` do,
        // so the inbox / steward can pick up the hand-off.
        if outcome.opened {
            self.emit_event(
                &repo.name(),
                "pull_request_opened",
                json!({
                    "branch": branch,
                    "target": target,
                    "url": result.get("pr_url"),
                    "detail": result.get("detail"),
                }),
            );
        }
        info!(branch, target, pr_opened = outcome.opened, "open_pr");
        Ok(result)
    }

    /// The live registry — archived records excluded. This is the default view
    /// every operator surface (`rk list`, `rk top`, `rk inbox`) and every sweep
    /// reads, so archiving a record retires it from all of them at once.
    pub fn list(&self) -> Vec<AgentRecord> {
        self.lock_registry().list().into_iter().cloned().collect()
    }

    /// Archived records only.
    pub fn list_archived(&self) -> Vec<AgentRecord> {
        self.lock_registry()
            .list_archived()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Live + archived: the full lifetime history, for cost/usage reporting.
    pub fn list_all(&self) -> Vec<AgentRecord> {
        self.lock_registry()
            .list_all()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Read-only status lookup, falling back to the archive so an archived
    /// rat's history stays inspectable with `rk status`.
    pub fn status(&self, name: &str) -> Option<AgentRecord> {
        self.lock_registry().get_any(name).cloned()
    }

    /// Move settled terminal records (`Completed`/`Failed`/`Dismissed`) last
    /// touched before `cutoff` into the archive store, so they stop inflating
    /// the default views. Live and `Orphaned` records are never touched.
    ///
    /// With `dry_run` the registry is not mutated at all — the reply lists what
    /// *would* move, so an operator can preview before committing. `reap`
    /// additionally reclaims the leftovers each archived agent scattered
    /// outside its record (see [`Reap`]).
    pub fn archive_agents(
        &self,
        cutoff: DateTime<Utc>,
        dry_run: bool,
        reap: Reap,
    ) -> rk_core::Result<serde_json::Value> {
        if dry_run {
            let eligible: Vec<AgentRecord> = self
                .lock_registry()
                .archivable(cutoff)
                .into_iter()
                .cloned()
                .collect();
            return Ok(json!({
                "dry_run": true,
                "count": eligible.len(),
                "agents": eligible,
                "reaped": [],
                "reaped_logs": [],
            }));
        }
        let archived = self.lock_registry().archive(cutoff)?;
        let done =
            |rows: &[serde_json::Value]| rows.iter().filter(|r| r["reaped"] == json!(true)).count();
        let reaped: Vec<serde_json::Value> = if reap.git {
            archived.iter().map(|r| self.reap_git(r)).collect()
        } else {
            Vec::new()
        };
        let reaped_logs: Vec<serde_json::Value> = if reap.logs {
            archived.iter().map(|r| self.reap_log(r)).collect()
        } else {
            Vec::new()
        };
        info!(
            count = archived.len(),
            cutoff = %cutoff,
            reaped = done(&reaped),
            reaped_logs = done(&reaped_logs),
            "archived terminal agent records"
        );
        Ok(json!({
            "dry_run": false,
            "count": archived.len(),
            "agents": archived,
            "reaped": reaped,
            "reaped_logs": reaped_logs,
        }))
    }

    /// Restore an archived record to the live registry (the undo for
    /// [`archive_agents`](Supervisor::archive_agents)).
    pub fn unarchive_agent(&self, name: &str) -> rk_core::Result<serde_json::Value> {
        let restored = self
            .lock_registry()
            .unarchive(name)?
            .ok_or_else(|| rk_core::Error::other(format!("no archived agent: {name}")))?;
        info!(agent = name, "unarchived agent record");
        Ok(json!({ "agent": restored }))
    }

    /// Reclaim one archived agent's git leftovers — its worktree and local
    /// branch — but ONLY when the branch has already landed in its target (or
    /// is already gone). An unmerged branch still holds the only copy of that
    /// rat's work, so it is left standing and reported as skipped; nothing here
    /// ever force-deletes unmerged work.
    ///
    /// Best-effort by construction: every failure becomes a `reaped: false` row
    /// with a reason rather than failing the archive that triggered it.
    fn reap_git(&self, record: &AgentRecord) -> serde_json::Value {
        let row = |reaped: bool, reason: String| json!({"agent": record.name, "branch": record.branch, "reaped": reaped, "reason": reason});
        let Some(branch) = record.branch.as_deref() else {
            return row(false, "no branch recorded".into());
        };
        let repo = match Repo::discover(&record.repo_root) {
            Ok(r) => r,
            Err(e) => return row(false, format!("repo unavailable: {e}")),
        };
        if !repo.branch_merged_or_gone(branch, &record.target_branch) {
            return row(
                false,
                format!(
                    "branch {branch} is not merged into {} — left standing",
                    record.target_branch
                ),
            );
        }
        let mut detail = Vec::new();
        if let Some(worktree) = &record.worktree {
            if worktree.exists() {
                match repo.remove_worktree(worktree) {
                    Ok(()) => detail.push("worktree removed".to_string()),
                    Err(e) => return row(false, format!("worktree removal failed: {e}")),
                }
            }
        }
        if repo.branch_exists(branch) {
            match repo.delete_branch(branch) {
                Ok(()) => detail.push(format!("branch {branch} deleted")),
                Err(e) => return row(false, format!("branch delete failed: {e}")),
            }
        } else {
            detail.push(format!("branch {branch} already gone"));
        }
        row(true, detail.join("; "))
    }

    /// Reclaim one archived agent's transcript: the `agent-logs/` file its own
    /// generation wrote, and nothing else. Each file is a bounded ring, but the
    /// count grows once per rat forever, so this is the sweep that keeps the
    /// directory finite.
    ///
    /// Unlike [`reap_git`](Supervisor::reap_git) there is no "has it settled
    /// yet" question to ask first — the branch holds the work, the transcript
    /// only narrates it — so every record that actually archives loses its own
    /// file. That makes this strictly one-way: `rk unarchive` restores the
    /// record but cannot bring the transcript back.
    ///
    /// Best-effort in the same shape as `reap_git`: a failure is a
    /// `reaped: false` row with a reason, never a failed archive.
    fn reap_log(&self, record: &AgentRecord) -> serde_json::Value {
        let row = |reaped: bool, reason: String| json!({"agent": record.name, "reaped": reaped, "reason": reason});
        match self.log.delete_for(&record.name, record.created_at) {
            Ok(true) => row(true, "transcript deleted".into()),
            Ok(false) => row(false, "no transcript on disk".into()),
            Err(e) => row(false, format!("transcript delete failed: {e}")),
        }
    }

    /// Standard spawn environment. Prepends the running `rk` binary's
    /// directory to PATH so the sugar commands work inside agent sessions.
    fn agent_env(
        &self,
        name: &str,
        role: &str,
        repo_name: &str,
        task: &str,
        branch: Option<&str>,
        worktree: &std::path::Path,
    ) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("RK_HOME".into(), self.layout.home().display().to_string());
        env.insert("RK_AGENT".into(), name.to_string());
        if let Ok(token) = self.layout.agent_auth_token(name) {
            env.insert("RK_AUTH_TOKEN".into(), token);
        }
        // So `rk prime` inside the rat renders this rat's own role automatically.
        env.insert("RK_ROLE".into(), role.to_string());
        env.insert("RK_REPO".into(), repo_name.to_string());
        env.insert("RK_TASK".into(), task.to_string());
        if let Some(branch) = branch {
            env.insert("RK_BRANCH".into(), branch.to_string());
        }
        env.insert("RK_WORKTREE".into(), worktree.display().to_string());
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                let path = std::env::var("PATH").unwrap_or_default();
                env.insert("PATH".into(), format!("{}:{path}", dir.display()));
            }
        }
        env
    }

    fn emit_event(&self, scope: &str, identity: &str, payload: serde_json::Value) {
        let tuple = Tuple::new(
            Category::Event,
            scope.to_string(),
            identity.to_string(),
            self.castle.clone(),
            payload,
        );
        if let Err(e) = self.space.out(tuple) {
            warn!(error = %e, identity, "failed to emit event tuple");
        }
    }

    fn lock_registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        match self.registry.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn lock_controls(&self) -> std::sync::MutexGuard<'_, HashMap<String, SessionControl>> {
        match self.controls.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn lock_sweep_state(&self) -> std::sync::MutexGuard<'_, HashMap<String, SweepState>> {
        match self.sweep_state.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn lock_completions(&self) -> std::sync::MutexGuard<'_, HashMap<String, CompletionState>> {
        match self.completions.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn lock_respawn_state(&self) -> std::sync::MutexGuard<'_, HashMap<String, RespawnState>> {
        match self.respawn_state.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
}

#[cfg(test)]
mod respawn_tests {
    use super::*;
    use rk_ledger::{Budget, FleetBudget};
    use std::path::Path;
    use std::process::Command;

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
        std::fs::write(dir.join("f"), "0\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-m", "init"]);
    }

    fn supervisor(home: &Path) -> Arc<Supervisor> {
        let layout = Layout::at(home);
        let tickets = Arc::new(crate::tickets::Tickets::new(
            Space::open_in_memory().unwrap(),
            "castle".into(),
        ));
        Arc::new(
            Supervisor::new(
                layout,
                "castle".into(),
                "fake".into(),
                Budget::default(),
                FleetBudget::default(),
                Space::open_in_memory().unwrap(),
                tickets,
            )
            .unwrap(),
        )
    }

    fn record(repo: &Path, branch: Option<&str>) -> AgentRecord {
        let now = Utc::now();
        AgentRecord {
            name: "Nibble".into(),
            role: "rat".into(),
            harness: "fake".into(),
            model: None,
            repo_root: repo.to_path_buf(),
            repo_name: "repo".into(),
            task: Some("t".into()),
            branch: branch.map(String::from),
            worktree: Some(repo.to_path_buf()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state: AgentState::Failed,
            crashed: false,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at: now,
            archived_at: None,
        }
    }

    /// The merged-branch guardrail is precise: a branch whose work landed (and
    /// whose target advanced past it) is skipped; a crashed-before-committing
    /// branch (tip == base) and an unmerged-work branch are both respawnable.
    #[test]
    fn guardrail_skips_only_genuinely_merged_branches() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());
        let p = repo.path();

        // (a) A branch that made a commit, then merged into a target that then
        // advanced past it => genuinely merged => skip.
        git(p, &["checkout", "-b", "merged", "main"]);
        std::fs::write(p.join("f"), "merged\n").unwrap();
        git(p, &["commit", "-am", "work"]);
        git(p, &["checkout", "main"]);
        std::fs::write(p.join("g"), "other\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "other-main"]);
        git(p, &["merge", "--no-ff", "-m", "merge", "merged"]);
        assert!(
            sup.branch_already_merged(&record(p, Some("merged"))),
            "a branch merged into an advanced target must be skipped"
        );

        // (b) A branch cut from main with NO commits (crashed before work):
        // tip == base, not strictly behind => respawnable.
        git(p, &["checkout", "-b", "nowork", "main"]);
        git(p, &["checkout", "main"]);
        assert!(
            !sup.branch_already_merged(&record(p, Some("nowork"))),
            "a no-commit branch (crashed early) must be respawnable"
        );

        // (c) A branch with commits NOT in the target => unmerged work => respawn.
        git(p, &["checkout", "-b", "unmerged", "main"]);
        std::fs::write(p.join("h"), "wip\n").unwrap();
        git(p, &["add", "."]);
        git(p, &["commit", "-m", "wip"]);
        git(p, &["checkout", "main"]);
        assert!(
            !sup.branch_already_merged(&record(p, Some("unmerged"))),
            "unmerged work must be respawnable"
        );

        // (d) A branch that no longer exists => nothing to resume => skip.
        assert!(
            sup.branch_already_merged(&record(p, Some("ghost"))),
            "a vanished branch must be skipped"
        );

        // (e) No branch recorded => not merged (fail-safe, respawn preflight handles it).
        assert!(!sup.branch_already_merged(&record(p, None)));
    }

    /// Backoff + cap: immediate first attempt, exponential wait between retries,
    /// escalate-once at the cap.
    #[test]
    fn decide_respawn_backs_off_then_caps() {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let sup = supervisor(home.path());
        let rec = record(repo.path(), Some("main"));
        let now = Utc::now();
        let cfg = SupervisorConfig {
            respawn_enabled: true,
            respawn_max_attempts: 3,
            respawn_backoff_secs: 10,
            ..SupervisorConfig::default()
        };

        // No prior state => first attempt fires immediately.
        assert!(matches!(
            sup.decide_respawn(&rec, now, &cfg),
            RespawnDecision::Respawn
        ));

        // After attempt 1, backoff = 10 * 2^0 = 10s: too soon => Wait.
        sup.record_respawn_attempt(&rec.name, now);
        assert!(matches!(
            sup.decide_respawn(&rec, now, &cfg),
            RespawnDecision::Wait
        ));
        // Past the 10s backoff => Respawn.
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(11), &cfg),
            RespawnDecision::Respawn
        ));

        // Attempt 2: backoff doubles to 20s. 15s is still too soon.
        sup.record_respawn_attempt(&rec.name, now);
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(15), &cfg),
            RespawnDecision::Wait
        ));

        // Reach the cap (attempts == max) => escalate once, then stay quiet.
        sup.record_respawn_attempt(&rec.name, now); // attempts now 3 == max
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(999), &cfg),
            RespawnDecision::Escalate
        ));
        sup.lock_respawn_state()
            .get_mut(&rec.name)
            .unwrap()
            .escalated = true;
        assert!(matches!(
            sup.decide_respawn(&rec, now + chrono::Duration::seconds(999), &cfg),
            RespawnDecision::Wait
        ));
    }
}
