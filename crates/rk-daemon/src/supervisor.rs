//! The supervisor: spawn rats into worktrees, pump their harness events into
//! the registry and tuplespace, route completions up the spawn tree, and
//! merge their work on dismissal.

use crate::agents::{AgentRecord, AgentState, Registry};
use chrono::Utc;
use rk_core::paths::Layout;
use rk_core::prime::{render, PrimeContext};
use rk_core::tuple::{Category, Tuple};
use rk_git::{agent_branch, Repo};
use rk_harness::{make_harness, HarnessEvent, LaunchSpec, SessionControl, TokenUsage};
use rk_ledger::pricing::PricingTable;
use rk_ledger::{Budget, BudgetAction};
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
}

fn default_role() -> String {
    "rat".into()
}

pub struct Supervisor {
    layout: Layout,
    castle: String,
    default_harness: String,
    registry: Mutex<Registry>,
    /// Live control handles (not persisted; gone after restart).
    controls: Mutex<HashMap<String, SessionControl>>,
    space: Space,
    pricing: PricingTable,
    budget: Budget,
    /// Agents already warned about budget (avoid repeat warnings).
    budget_warned: Mutex<std::collections::HashSet<String>>,
}

impl Supervisor {
    pub fn new(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: Budget,
        space: Space,
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
        Ok(Self {
            layout,
            castle,
            default_harness,
            registry: Mutex::new(registry),
            controls: Mutex::new(HashMap::new()),
            space,
            pricing,
            budget,
            budget_warned: Mutex::new(std::collections::HashSet::new()),
        })
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
        let target_branch = match &params.base {
            Some(b) => b.clone(),
            None => repo.current_branch()?,
        };

        let name = {
            let registry = self.lock_registry();
            rk_core::names::next_name(registry.names_in_use())
        };
        let branch = agent_branch(&name, &params.task);
        let worktree = self.layout.worktrees_dir().join(&repo_name).join(&name);
        repo.create_worktree(&worktree, &branch, &target_branch)?;

        let harness_kind = params
            .harness
            .clone()
            .unwrap_or_else(|| self.default_harness.clone());
        let harness = match make_harness(&harness_kind) {
            Ok(h) => h,
            Err(e) => {
                let _ = repo.remove_worktree(&worktree);
                let _ = repo.delete_branch(&branch);
                return Err(e);
            }
        };

        let prime_ctx = PrimeContext {
            agent: name.clone(),
            repo: repo_name.clone(),
            task: Some(params.task.clone()),
            branch: Some(branch.clone()),
            parent: params.parent.clone(),
        };
        let prompt = params
            .prompt
            .clone()
            .unwrap_or_else(|| format!("Work on task {}. Begin now.", params.task));

        let mut env = self.agent_env(&name, &repo_name, &params.task, Some(&branch), &worktree);
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
            session_id: None,
            attach_target: None,
            pid: session.pid,
            state: AgentState::Running,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
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
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&name, event);
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
            session_id: None,
            attach_target: Some(target.clone()),
            pid: None,
            state: AgentState::Running,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
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
            tokio::spawn(async move {
                let mut pattern =
                    rk_core::tuple::Pattern::category(Category::Event).identity("task_done");
                pattern.payload_search = Some(format!("\"agent\":\"{agent}\""));
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
                            supervisor.route_completion(&record, false);
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
            })?
            .ok_or_else(|| rk_core::Error::other("record vanished"))?;
        self.lock_controls()
            .insert(name.to_string(), session.control.clone());

        self.emit_event(
            &updated.repo_name,
            "agent_respawned",
            json!({"agent": name, "task": updated.task}),
        );

        let supervisor = Arc::clone(self);
        let owned = name.to_string();
        let mut events = session.events;
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                supervisor.handle_event(&owned, event);
            }
        });
        Ok(updated)
    }

    fn handle_event(self: &Arc<Self>, name: &str, event: HarnessEvent) {
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
                    info!(agent = name, is_error, "agent completed");
                    self.route_completion(&record, is_error);
                }
            }
            HarnessEvent::Exited { code } => {
                self.lock_controls().remove(name);
                let _ = self.lock_registry().update(name, |r| {
                    r.pid = None;
                    // Exit without a Completed event = crash/kill.
                    if r.state.is_live() {
                        r.state = AgentState::Failed;
                        r.result =
                            Some(format!("process exited (code {code:?}) without completing"));
                    }
                });
            }
            HarnessEvent::AssistantText { .. }
            | HarnessEvent::ToolUse { .. }
            | HarnessEvent::Retry { .. } => {}
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
        if let Err(e) = self.space.out(tuple) {
            warn!(error = %e, "failed to emit budget obstacle");
        }
    }

    /// Route a completion up the spawn tree: the structural parent gets a
    /// directed message; the repo scope gets the event either way.
    fn route_completion(&self, record: &AgentRecord, is_error: bool) {
        self.emit_event(
            &record.repo_name,
            "harness_result",
            json!({
                "agent": record.name,
                "task": record.task,
                "branch": record.branch,
                "parent": record.parent,
                "is_error": is_error,
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

    /// Dismiss: stop the session if live, merge the branch into the target,
    /// remove the worktree, and (if merged) delete the branch.
    pub async fn dismiss(&self, name: &str, no_merge: bool) -> rk_core::Result<serde_json::Value> {
        let record = self
            .lock_registry()
            .get(name)
            .cloned()
            .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))?;

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
        let mut detail = String::from("no merge requested");

        if let Some(worktree) = &record.worktree {
            if worktree.exists() {
                repo.remove_worktree(worktree)?;
            }
        }
        if let Some(branch) = &record.branch {
            if !no_merge {
                let outcome = repo.merge_branch(branch, &record.target_branch)?;
                merged = outcome.merged;
                detail = outcome.detail;
                if merged {
                    repo.delete_branch(branch)?;
                }
            } else {
                detail = format!("branch {branch} preserved (--no-merge)");
            }
        }

        self.lock_registry().update(name, |r| {
            r.state = AgentState::Dismissed;
            r.pid = None;
        })?;
        self.emit_event(
            &record.repo_name,
            "agent_dismissed",
            json!({"agent": name, "merged": merged, "detail": detail, "parent": record.parent}),
        );
        Ok(json!({"agent": name, "merged": merged, "detail": detail}))
    }

    pub fn list(&self) -> Vec<AgentRecord> {
        self.lock_registry().list().into_iter().cloned().collect()
    }

    pub fn status(&self, name: &str) -> Option<AgentRecord> {
        self.lock_registry().get(name).cloned()
    }

    /// Standard spawn environment. Prepends the running `rk` binary's
    /// directory to PATH so the sugar commands work inside agent sessions.
    fn agent_env(
        &self,
        name: &str,
        repo_name: &str,
        task: &str,
        branch: Option<&str>,
        worktree: &std::path::Path,
    ) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("RK_HOME".into(), self.layout.home().display().to_string());
        env.insert("RK_AGENT".into(), name.to_string());
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
}
