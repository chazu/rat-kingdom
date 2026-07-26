//! Workflow execution: sequential step machine over the supervisor and the
//! tuplespace. Definitions come from rk-workflow (cue CLI); this module owns
//! instances, context threading, and step semantics.

use crate::supervisor::{SpawnParams, Supervisor};
use crate::tickets::Tickets;
use chrono::{DateTime, Utc};
use rk_core::id::prefixed_id;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Pattern};
use rk_space::Space;
use rk_workflow::{
    resolve::{resolve, resolve_fields},
    AgentProfile, DismissAllStep, ForEachStep, RunStep, Step, SubWorkflowStep, TicketQuery,
    TierRouting, WaitAllStep, Workflow,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

/// A boxed future for hand-rolled async recursion (nested `when` / `repeat`).
type StepFuture<'a> = Pin<Box<dyn Future<Output = rk_core::Result<Flow>> + Send + 'a>>;

/// Mirrors rk-workflow's `RunStep` timeout default; a referencing `run` step
/// left at this value defers to a named check's own timeout (TKT-30).
const DEFAULT_RUN_TIMEOUT: &str = "10m";

/// Hard ceiling on `sub_workflow` nesting depth — the depth analog of the
/// `repeat` max cap (rk-workflow `#RepeatStep.max`). A top-level `run` is depth
/// 0; each nested `sub_workflow` is one deeper. A workflow cycle (A→B→A…) hits
/// this cap and fails closed rather than recursing until it exhausts the stack.
const MAX_SUBWORKFLOW_DEPTH: usize = 8;

/// The effective parameters of a `run` step after named-check resolution and
/// policy enforcement — a raw command or a repo-registered check collapse to the
/// same shape here.
struct ResolvedRun {
    command: String,
    cwd: Option<String>,
    expect_exit: Option<i64>,
    timeout: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    pub workflow: String,
    pub repo: String,
    pub status: InstanceStatus,
    pub current_step: usize,
    pub total_steps: usize,
    pub context: WorkflowContext,
    #[serde(default)]
    pub error: Option<String>,
    /// Set while the instance is blocked awaiting a human decision at an
    /// approval gate; cleared once the decision (or timeout) arrives. This is
    /// the precise "parked at a gate" signal `rk inbox` reports — a `Running`
    /// status alone can't distinguish a parked gate from active execution.
    #[serde(default)]
    pub awaiting: Option<String>,
    /// Per-instance budget cap in USD from the workflow's `budget:` field.
    /// Once this instance's summed agent cost reaches it, further dispatch
    /// (single spawn or fan-out) is refused. `None`/0 = unlimited.
    #[serde(default)]
    pub instance_max_usd: Option<f64>,
    /// The definition name/path `run` was invoked with, used to relocate and
    /// reload the workflow when resuming after a restart (TKT-52). Persisted so
    /// a rehydrated instance can re-`load` the exact same steps.
    #[serde(default)]
    pub definition: String,
    /// The original `_input` params this instance launched with, replayed at
    /// reload so a resumed workflow validates and interpolates identically to
    /// its first run (TKT-52).
    #[serde(default)]
    pub params: HashMap<String, Value>,
    /// Sub-workflow nesting depth: 0 for a top-level `run`, incremented for each
    /// enclosing `sub_workflow` step (TKT-57). Bounded by
    /// [`MAX_SUBWORKFLOW_DEPTH`] — the depth analog of the `repeat` max cap — so
    /// a workflow cycle fails closed instead of recursing without end.
    #[serde(default)]
    pub depth: usize,
    pub started_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkflowContext {
    pub active_agent: Option<String>,
    pub active_branch: Option<String>,
    pub previous_result: Option<Value>,
    /// Values lifted from the space by `read` steps, keyed by `read.into`.
    /// Consumed by `when` steps and by `{{ctx.var.<name>}}` interpolation.
    #[serde(default)]
    pub vars: HashMap<String, Value>,
    /// Agents spawned by the most recent fan-out (`for_each`), awaiting a
    /// `wait_all` join. This is the fan-out counterpart to `active_agent`:
    /// sequential steps keep using `active_agent`; fan-out steps use this list
    /// so the single-active-agent path stays untouched.
    #[serde(default)]
    pub fanout: Vec<FannedAgent>,
}

/// One agent in a fan-out set: its name, its branch, and the ticket it drains.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FannedAgent {
    pub agent: String,
    pub branch: Option<String>,
    pub ticket: Option<String>,
}

/// Control-flow signal threaded out of a step (or nested step sequence).
enum Flow {
    /// Continue with the next step in sequence.
    Next,
    /// Exit the nearest enclosing `repeat` (or end the workflow at top level).
    Break,
}

pub struct WorkflowEngine {
    layout: Layout,
    supervisor: Arc<Supervisor>,
    space: Space,
    tickets: Arc<Tickets>,
    global_agents: HashMap<String, AgentProfile>,
    /// Global cost-tier routing; a workflow's own `tiers:` table shadows it.
    tier_routing: TierRouting,
    default_harness: String,
    /// When set, a `run` step may only invoke a repo-registered named check; a
    /// raw inline command is refused fail-closed (TKT-30, `[policy]`).
    require_named_checks: bool,
    instances: Mutex<HashMap<String, Instance>>,
}

impl WorkflowEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        layout: Layout,
        supervisor: Arc<Supervisor>,
        space: Space,
        tickets: Arc<Tickets>,
        global_agents: HashMap<String, AgentProfile>,
        tier_routing: TierRouting,
        default_harness: String,
        require_named_checks: bool,
    ) -> Self {
        Self {
            layout,
            supervisor,
            space,
            tickets,
            global_agents,
            tier_routing,
            default_harness,
            require_named_checks,
            instances: Mutex::new(HashMap::new()),
        }
    }

    /// Resolve `<name>` to a definition file: `<repo>/.rk/workflows/<name>.cue`
    /// wins over `~/.rat-kingdom/workflows/<name>.cue`; a path is used as-is.
    pub fn find_definition(&self, name: &str, repo: &str) -> rk_core::Result<PathBuf> {
        let as_path = PathBuf::from(name);
        if as_path.extension().map(|e| e == "cue").unwrap_or(false) && as_path.exists() {
            return Ok(as_path);
        }
        let repo_local = PathBuf::from(repo)
            .join(".rk")
            .join("workflows")
            .join(format!("{name}.cue"));
        if repo_local.exists() {
            return Ok(repo_local);
        }
        let global = self.layout.workflows_dir().join(format!("{name}.cue"));
        if global.exists() {
            return Ok(global);
        }
        Err(rk_core::Error::other(format!(
            "no workflow named '{name}' (looked in {} and {})",
            repo_local.display(),
            global.display()
        )))
    }

    pub fn definitions(&self, repo: &str) -> Vec<String> {
        let mut names: Vec<String> = rk_workflow::definitions(&self.layout.workflows_dir())
            .into_iter()
            .chain(rk_workflow::definitions(
                &PathBuf::from(repo).join(".rk").join("workflows"),
            ))
            .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Load, validate, and launch a workflow. Returns the instance snapshot;
    /// execution continues in a background task.
    pub fn run(
        self: &Arc<Self>,
        name: &str,
        repo: &str,
        params: HashMap<String, Value>,
    ) -> rk_core::Result<Instance> {
        let file = self.find_definition(name, repo)?;
        let workflow = rk_workflow::load(&file, &params)?;

        let instance = Instance {
            id: prefixed_id("wf"),
            workflow: workflow.name.clone(),
            repo: repo.to_string(),
            status: InstanceStatus::Running,
            current_step: 0,
            total_steps: workflow.steps.len(),
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: workflow.budget.map(|b| b.max_usd),
            definition: name.to_string(),
            params,
            depth: 0,
            started_at: chrono::Utc::now(),
            completed_at: None,
        };
        self.store(instance.clone());
        self.spawn_execution(instance.id.clone(), workflow, repo.to_string());
        Ok(instance)
    }

    /// Drive an instance's steps to completion on a background task, then record
    /// the terminal status and emit the completion/failure event. Shared by a
    /// fresh `run` and a post-restart `resume`, so both paths finalize
    /// identically.
    fn spawn_execution(self: &Arc<Self>, id: String, workflow: Workflow, repo: String) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let result = engine.execute(&id, workflow, &repo).await;
            // The instance record carries the workflow name for the event; read
            // it back rather than threading it through the moved `workflow`.
            let workflow_name = engine.status(&id).map(|i| i.workflow).unwrap_or_default();
            engine.finalize(&id, &repo, &workflow_name, result);
        });
    }

    /// Record an instance's terminal status and broadcast its completion event.
    fn finalize(&self, id: &str, repo: &str, workflow_name: &str, result: rk_core::Result<()>) {
        let (status, error) = match result {
            Ok(()) => (InstanceStatus::Completed, None),
            Err(e) => (InstanceStatus::Failed, Some(e.to_string())),
        };
        self.update(id, |i| {
            i.status = status;
            i.error = error.clone();
            i.completed_at = Some(chrono::Utc::now());
        });
        let final_status = if status == InstanceStatus::Completed {
            "workflow_complete"
        } else {
            "workflow_failed"
        };
        info!(instance = %id, status = ?status, "workflow finished");
        let _ = self.space.out(rk_core::tuple::Tuple::new(
            Category::Event,
            repo_name_of(repo),
            final_status,
            "daemon".to_string(),
            json!({"instance": id, "workflow": workflow_name, "error": error}),
        ));
    }

    /// Load persisted instances from disk on daemon startup (TKT-52).
    ///
    /// Every mutation already writes each instance to
    /// `<home>/workflow-instances/<id>.json`; this is the missing read side.
    /// Completed and failed instances are restored for history — so
    /// `rk workflow status`/`list` and `rk approve` survive a restart — while
    /// `Running` instances are additionally *resumed*: re-executed from their
    /// persisted step cursor so a crash or restart mid-run no longer silently
    /// drops an in-flight workflow (a parked approval gate, a fan-out waiting on
    /// `wait_all`). Idempotent: instances already in memory are overwritten by
    /// their on-disk snapshot, so calling it twice is harmless.
    pub fn rehydrate(self: &Arc<Self>) {
        let dir = self.layout.home().join("workflow-instances");
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            // No instances persisted yet — a fresh home. Nothing to restore.
            Err(_) => return,
        };
        let mut resumable = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let instance: Instance = match std::fs::read(&path)
                .ok()
                .and_then(|data| serde_json::from_slice(&data).ok())
            {
                Some(i) => i,
                None => {
                    warn!(path = %path.display(), "skipping unreadable workflow instance file");
                    continue;
                }
            };
            // Only top-level (depth 0) instances resume standalone. A nested
            // sub-workflow child (depth > 0) is re-driven by its parent's
            // resumed `sub_workflow` step (which re-runs the interrupted step and
            // launches a fresh child), so resuming it independently here would
            // double-run its agents. It is still loaded into memory for history.
            let running = instance.status == InstanceStatus::Running && instance.depth == 0;
            self.lock().insert(instance.id.clone(), instance.clone());
            if running {
                resumable.push(instance);
            }
        }
        if !resumable.is_empty() {
            info!(
                count = resumable.len(),
                "resuming in-flight workflow instances after restart"
            );
        }
        for instance in resumable {
            self.resume(instance);
        }
    }

    /// Resume one rehydrated `Running` instance: reload its definition with the
    /// original params and continue execution from the persisted step cursor. A
    /// definition that no longer loads (deleted, or now invalid) fails the
    /// instance cleanly — surfaced in `rk inbox` — rather than leaving it wedged
    /// `Running` forever.
    fn resume(self: &Arc<Self>, instance: Instance) {
        let id = instance.id.clone();
        let workflow = match self
            .find_definition(&instance.definition, &instance.repo)
            .and_then(|file| rk_workflow::load(&file, &instance.params))
        {
            Ok(w) => w,
            Err(e) => {
                warn!(instance = %id, error = %e, "cannot resume workflow; failing instance");
                self.update(&id, |i| {
                    i.status = InstanceStatus::Failed;
                    i.error = Some(format!("resume failed: could not reload definition: {e}"));
                    i.awaiting = None;
                    i.completed_at = Some(chrono::Utc::now());
                });
                return;
            }
        };
        // A stale `awaiting` flag from before the restart is cleared here; the
        // resumed gate re-sets it if it parks again.
        self.update(&id, |i| i.awaiting = None);
        info!(instance = %id, from_step = instance.current_step, "resuming workflow after restart");
        self.spawn_execution(id, workflow, instance.repo);
    }

    /// Run the top-level step list once. `current_step` is the resume cursor:
    /// the count of top-level steps that have fully COMPLETED, i.e. the index of
    /// the next step to run. Steps already completed before a restart are
    /// skipped; the step that was in flight when the daemon stopped re-runs
    /// (at-least-once for the interrupted step). Steps nested inside
    /// `when`/`repeat` execute in place without advancing the cursor (they are
    /// bounded by the `repeat` cap), so a resume inside a loop re-enters the
    /// whole enclosing top-level step.
    async fn execute(&self, id: &str, workflow: Workflow, repo: &str) -> rk_core::Result<()> {
        let start = self.lock().get(id).map(|i| i.current_step).unwrap_or(0);
        for (index, step) in workflow.steps.iter().enumerate() {
            if index < start {
                // Already completed on a prior run; do not re-execute it.
                continue;
            }
            if let Flow::Break = self
                .run_step(id, step, repo, &workflow.agents, &workflow.tiers)
                .await?
            {
                // A top-level break ends the workflow (nothing to loop out of).
                break;
            }
            // Advance only AFTER the step completes, so a restart resumes at the
            // interrupted step and never re-runs a finished one.
            self.update(id, |i| i.current_step = index + 1);
        }
        Ok(())
    }

    /// Run a sequence of steps, short-circuiting on the first `Break`.
    fn run_steps<'a>(
        &'a self,
        id: &'a str,
        steps: &'a [Step],
        repo: &'a str,
        agents: &'a HashMap<String, AgentProfile>,
        tiers: &'a TierRouting,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            for step in steps {
                if let Flow::Break = self.run_step(id, step, repo, agents, tiers).await? {
                    return Ok(Flow::Break);
                }
            }
            Ok(Flow::Next)
        })
    }

    /// Execute a single step (recursing for `when`/`repeat`). `tiers` is the
    /// workflow's own tier-routing table, chained over the global one at fan-out.
    fn run_step<'a>(
        &'a self,
        id: &'a str,
        step: &'a Step,
        repo: &'a str,
        agents: &'a HashMap<String, AgentProfile>,
        tiers: &'a TierRouting,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let ctx = self.context(id);
            match step {
                Step::Spawn(spawn) => {
                    let resolved =
                        resolve(spawn, agents, &self.global_agents, &self.default_harness)?;
                    let title = interpolate(&spawn.task.title, &ctx);
                    let prompt = spawn
                        .task
                        .description
                        .as_ref()
                        .map(|d| interpolate(d, &ctx));
                    let record = self.supervisor.spawn(SpawnParams {
                        repo: repo.to_string(),
                        task: title,
                        prompt,
                        role: spawn.role.clone(),
                        harness: Some(resolved.harness),
                        parent: None,
                        base: spawn.branch.clone().or(ctx.active_branch.clone()),
                        model: resolved.model,
                        permission_mode: resolved.permission_mode,
                        attach: false,
                        workflow_instance: Some(id.to_string()),
                        instance_max_usd: self.instance_budget(id),
                    })?;
                    self.update(id, |i| {
                        i.context.active_agent = Some(record.name.clone());
                        i.context.active_branch = record.branch.clone();
                    });
                }
                Step::Wait(wait) => {
                    let agent = ctx
                        .active_agent
                        .clone()
                        .ok_or_else(|| rk_core::Error::other("wait step with no active agent"))?;
                    let timeout = parse_duration(&wait.timeout)?;
                    let pattern = self.result_pattern(id, &agent);
                    let tuple = self
                        .space
                        .rd(&pattern, timeout)
                        .await
                        .map_err(|e| rk_core::Error::other(format!("wait failed: {e}")))?
                        .ok_or_else(|| {
                            rk_core::Error::other(format!(
                                "wait timed out after {} for agent {agent}",
                                wait.timeout
                            ))
                        })?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(tuple.payload.clone());
                    });
                }
                Step::Evaluate(eval) => {
                    let actual = ctx.previous_result.clone().unwrap_or(Value::Null);
                    // Pass if the result unifies with `expect` OR any `anyOf`
                    // alternative — a disjunction single-`expect` unification (an
                    // AND over fields) cannot express. Short-circuits on the
                    // first match.
                    let mut passed = rk_workflow::unify_concrete(&eval.expect, &actual)?;
                    for alt in &eval.any_of {
                        if passed {
                            break;
                        }
                        passed = rk_workflow::unify_concrete(alt, &actual)?;
                    }
                    if !passed {
                        return Err(rk_core::Error::other(format!(
                            "evaluate failed: expect {} (anyOf {:?}) did not unify with {}",
                            eval.expect, eval.any_of, actual
                        )));
                    }
                }
                Step::Dismiss(dismiss) => {
                    let agent = ctx.active_agent.clone().ok_or_else(|| {
                        rk_core::Error::other("dismiss step with no active agent")
                    })?;
                    let outcome = self.supervisor.dismiss(&agent, dismiss.no_merge).await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(outcome.clone());
                        i.context.active_agent = None;
                    });
                }
                Step::Gate(gate) => match gate.gate_type.as_str() {
                    "timer" => {
                        let duration = gate
                            .duration
                            .as_deref()
                            .ok_or_else(|| rk_core::Error::other("timer gate missing duration"))?;
                        tokio::time::sleep(parse_duration(duration)?).await;
                    }
                    "approval" => {
                        // Block until a human decision for THIS instance arrives
                        // (via `rk approve`/`rk reject`, which write a
                        // `workflow_approval` event) or the timeout elapses.
                        let timeout = parse_duration(gate.timeout.as_deref().unwrap_or("24h"))?;
                        let mut pattern =
                            Pattern::category(Category::Event).identity("workflow_approval");
                        // Scope the wait to this instance. serde_json renders the
                        // pair contiguously regardless of key order, so this
                        // substring is a reliable per-instance predicate.
                        pattern.payload_search = Some(format!("\"instance\":\"{id}\""));
                        // Flag the instance as parked so `rk inbox` can surface
                        // it with the `rk approve`/`rk reject` resolving command.
                        self.update(id, |i| i.awaiting = Some("approval".to_string()));
                        let read = self.space.rd(&pattern, timeout).await;
                        self.update(id, |i| i.awaiting = None);
                        let decision = match read.map_err(|e| {
                            rk_core::Error::other(format!("approval gate failed: {e}"))
                        })? {
                            Some(tuple) => tuple.payload,
                            None => {
                                // Fail closed: no human response means no merge.
                                // Record the synthetic decision as a
                                // workflow_approval event too, so a following
                                // `read`/`when` routes the timeout down the same
                                // clean reject path as an explicit rejection —
                                // rather than the read blocking on a tuple that
                                // never arrives.
                                let payload = json!({
                                    "instance": id,
                                    "approved": false,
                                    "by": "system",
                                    "reason": format!("no approval within {}", gate.timeout.as_deref().unwrap_or("24h")),
                                });
                                let _ = self.space.out(rk_core::tuple::Tuple::new(
                                    Category::Event,
                                    repo_name_of(repo),
                                    "workflow_approval",
                                    "system".to_string(),
                                    payload.clone(),
                                ));
                                payload
                            }
                        };
                        self.update(id, |i| {
                            i.context.previous_result = Some(decision);
                        });
                    }
                    other => {
                        return Err(rk_core::Error::other(format!("unknown gate type: {other}")));
                    }
                },
                Step::Read(read) => {
                    let category = Category::from_str(&read.category)?;
                    let scope = read.scope.clone().unwrap_or_else(|| repo_name_of(repo));
                    // `fromAgent` narrows the read to the tuple THIS instance's
                    // active agent wrote (TKT-161). Without it the predicate is
                    // (category, scope, identity), which two instances of one
                    // workflow on one repo share by construction — the reactor
                    // fires `steward` per rat completion, so concurrent
                    // reviewers write `artifact/<repo>/review` at the same time
                    // and "newest wins" can hand a steward the OTHER steward's
                    // verdict to route a land on. Same failure shape as TKT-146
                    // (a read satisfied by a record that is not the one being
                    // waited on), so it takes the same cure: the agent's name
                    // plus its generation floor, via `for_agent_since`.
                    let mut pattern = if read.from_agent {
                        if read.search.is_some() {
                            return Err(rk_core::Error::other(
                                "read step sets both `fromAgent` and `search`; they claim the \
                                 same payload predicate — drop one",
                            ));
                        }
                        let agent = ctx.active_agent.clone().ok_or_else(|| {
                            rk_core::Error::other(
                                "read step has `fromAgent` but no active agent; only a step \
                                 after a `spawn` can bind a read to its author",
                            )
                        })?;
                        Pattern::for_agent_since(
                            category,
                            read.identity.clone(),
                            &agent,
                            self.generation_floor(id, &agent),
                        )
                    } else {
                        let mut pattern =
                            Pattern::category(category).identity(read.identity.clone());
                        pattern.payload_search = read.search.clone();
                        pattern
                    };
                    pattern.scope = Some(scope);
                    // Newest match wins (scan is oldest-first, so pop the tail);
                    // fall back to a blocking read if none is present yet.
                    let tuple = match self
                        .space
                        .scan(&pattern)
                        .map_err(|e| rk_core::Error::other(format!("read scan failed: {e}")))?
                        .pop()
                    {
                        Some(t) => Some(t),
                        None => self
                            .space
                            .rd(&pattern, parse_duration(&read.timeout)?)
                            .await
                            .map_err(|e| rk_core::Error::other(format!("read failed: {e}")))?,
                    };
                    let tuple = tuple.ok_or_else(|| {
                        // Name the binding in the failure: under `fromAgent` the
                        // usual cause is an agent that wrote its tuple without
                        // its own name in the payload, which reads as "nothing
                        // matched" and is otherwise indistinguishable from an
                        // agent that never wrote one at all.
                        let bound_to = match (read.from_agent, ctx.active_agent.as_deref()) {
                            (true, Some(agent)) => format!(" written by {agent}"),
                            _ => String::new(),
                        };
                        rk_core::Error::other(format!(
                            "read timed out after {} for {} tuple '{}'{bound_to}",
                            read.timeout, read.category, read.identity
                        ))
                    })?;
                    let value = match &read.field {
                        Some(field) => tuple.payload.get(field).cloned().unwrap_or(Value::Null),
                        None => tuple.payload.clone(),
                    };
                    self.update(id, |i| {
                        i.context.vars.insert(read.into.clone(), value.clone());
                    });
                }
                Step::When(when) => {
                    let key = ctx
                        .vars
                        .get(&when.var)
                        .map(value_as_key)
                        .unwrap_or_default();
                    let branch = when.cases.get(&key).unwrap_or(&when.default);
                    return self.run_steps(id, branch, repo, agents, tiers).await;
                }
                Step::Repeat(repeat) => {
                    for _ in 0..repeat.max {
                        if let Flow::Break = self
                            .run_steps(id, &repeat.steps, repo, agents, tiers)
                            .await?
                        {
                            break;
                        }
                    }
                }
                Step::Break => return Ok(Flow::Break),
                Step::Stop(stop) => {
                    return Err(rk_core::Error::other(format!(
                        "workflow stopped: {}",
                        stop.reason.as_deref().unwrap_or("stop step reached")
                    )));
                }
                Step::ForEach(fe) => {
                    let fanout = self.fan_out(id, agents, tiers, repo, fe).await?;
                    self.update(id, |i| i.context.fanout = fanout);
                }
                Step::WaitAll(wait_all) => {
                    let summary = self.join(id, &ctx.fanout, wait_all).await?;
                    self.update(id, |i| i.context.previous_result = Some(summary.clone()));
                }
                Step::DismissAll(dismiss_all) => {
                    let summary = self
                        .dismiss_fanout(&ctx.fanout, dismiss_all, ctx.previous_result.as_ref())
                        .await?;
                    self.update(id, |i| {
                        i.context.previous_result = Some(summary.clone());
                        // The fan-out set is spent once its branches are merged.
                        i.context.fanout = Vec::new();
                    });
                }
                Step::Run(run) => {
                    let result = self.run_command(&ctx, run, repo).await?;
                    self.update(id, |i| i.context.previous_result = Some(result.clone()));
                }
                Step::Land(land) => {
                    let branch = interpolate(&land.branch, &ctx);
                    let target = interpolate(&land.target, &ctx);
                    if branch.is_empty() {
                        return Err(rk_core::Error::other(
                            "land step: branch resolved to empty (no branch to land — did an \
                             earlier step set {{ctx.activeBranch}}?)",
                        ));
                    }
                    if target.is_empty() {
                        return Err(rk_core::Error::other("land step: target resolved to empty"));
                    }
                    let result = self
                        .supervisor
                        .land(std::path::Path::new(repo), &branch, &target, land.keep_branch)
                        .await?;
                    self.update(id, |i| i.context.previous_result = Some(result.clone()));
                }
                Step::OpenPr(open_pr) => {
                    let branch = interpolate(&open_pr.branch, &ctx);
                    let target = interpolate(&open_pr.target, &ctx);
                    if branch.is_empty() {
                        return Err(rk_core::Error::other(
                            "open_pr step: branch resolved to empty (no branch to open a PR for — \
                             did an earlier step set {{ctx.activeBranch}}?)",
                        ));
                    }
                    if target.is_empty() {
                        return Err(rk_core::Error::other(
                            "open_pr step: target resolved to empty",
                        ));
                    }
                    let result = self
                        .supervisor
                        .open_pr(std::path::Path::new(repo), &branch, &target)
                        .await?;
                    self.update(id, |i| i.context.previous_result = Some(result.clone()));
                }
                Step::SubWorkflow(sub) => {
                    let result = self.run_sub_workflow(id, sub, repo, &ctx).await?;
                    self.update(id, |i| i.context.previous_result = Some(result.clone()));
                }
            }
            Ok(Flow::Next)
        })
    }

    /// Run another workflow inline as a step — composition (TKT-57). Resolves and
    /// loads the named definition exactly like a top-level `run` (params
    /// templated from the parent's ctx, then coerced to the child's declared
    /// types), executes it to completion on THIS task (the parent step blocks on
    /// it), and returns the child's final `ctx.previous_result` so the caller can
    /// join it into the parent's context for a following `evaluate`/`when`.
    ///
    /// The child gets its own persisted [`Instance`] and its own
    /// `workflow_complete`/`workflow_failed` event via [`finalize`], so it shows
    /// up in `rk workflow list`/`status` and (on failure) `rk inbox` just like a
    /// directly-run workflow — the one difference being that its result flows
    /// back to a parent. Its budget/agents come from its own definition, so
    /// running B as a sub-step behaves like running B directly.
    ///
    /// Nesting is bounded by [`MAX_SUBWORKFLOW_DEPTH`]: a child one deeper than
    /// its parent, refused fail-closed past the cap. This is the depth analog of
    /// the `repeat` max cap and is what keeps a workflow cycle (A→B→A…) finite.
    /// A child failure is propagated as this step's error (fail-closed).
    async fn run_sub_workflow(
        &self,
        parent_id: &str,
        sub: &SubWorkflowStep,
        repo: &str,
        ctx: &WorkflowContext,
    ) -> rk_core::Result<Value> {
        let parent_depth = self.lock().get(parent_id).map(|i| i.depth).unwrap_or(0);
        let depth = parent_depth + 1;
        if depth > MAX_SUBWORKFLOW_DEPTH {
            return Err(rk_core::Error::other(format!(
                "sub_workflow nesting too deep (depth {depth} > cap {MAX_SUBWORKFLOW_DEPTH}): \
                 refusing to run '{}' — a workflow cycle? (depth guard, the analog of the \
                 repeat max cap)",
                sub.workflow
            )));
        }
        // Repo defaults to the parent's; a child may target another registered
        // repo/path when set.
        let child_repo = sub.repo.clone().unwrap_or_else(|| repo.to_string());
        // Interpolate each param against the parent's ctx, then hand them to the
        // loader as strings — coerced to the child's declared `#Param` types
        // exactly like reactor-templated params (a single `--param k=v` is a
        // string too). Forward a parent param with CUE interpolation in the def.
        let params: HashMap<String, Value> = sub
            .params
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(interpolate(v, ctx))))
            .collect();
        let file = self.find_definition(&sub.workflow, &child_repo)?;
        let workflow = rk_workflow::load(&file, &params)?;
        let workflow_name = workflow.name.clone();
        let child = Instance {
            id: prefixed_id("wf"),
            workflow: workflow_name.clone(),
            repo: child_repo.clone(),
            status: InstanceStatus::Running,
            current_step: 0,
            total_steps: workflow.steps.len(),
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: workflow.budget.map(|b| b.max_usd),
            definition: sub.workflow.clone(),
            params,
            depth,
            started_at: chrono::Utc::now(),
            completed_at: None,
        };
        let child_id = child.id.clone();
        self.store(child);
        info!(parent = %parent_id, child = %child_id, workflow = %workflow_name, depth, "running sub-workflow inline");
        // Execute the child on this task so the parent step joins on it. finalize
        // records the terminal status and emits the child's own completion event,
        // identical to a top-level run.
        match self.execute(&child_id, workflow, &child_repo).await {
            Ok(()) => {
                self.finalize(&child_id, &child_repo, &workflow_name, Ok(()));
                // The child's final result is this sub_workflow's return value.
                Ok(self
                    .status(&child_id)
                    .and_then(|i| i.context.previous_result)
                    .unwrap_or(Value::Null))
            }
            Err(e) => {
                let msg = e.to_string();
                self.finalize(
                    &child_id,
                    &child_repo,
                    &workflow_name,
                    Err(rk_core::Error::other(msg.clone())),
                );
                Err(rk_core::Error::other(format!(
                    "sub_workflow '{}' (instance {child_id}) failed: {msg}",
                    sub.workflow
                )))
            }
        }
    }

    /// Enumerate the matching tickets and spawn one agent per ticket in
    /// parallel, returning the fan-out set. The task title defaults to the
    /// ticket id, so the supervisor owns each ticket's status lifecycle exactly
    /// as it does for any ticket-dispatched rat (→ `done` on a clean finish,
    /// → `closed` on merge).
    ///
    /// Each ticket is atomically claimed (`open` → `in_progress`) via
    /// `tickets.claim` *before* its agent spawns, so two concurrent drains
    /// never grab the same ticket — the loser simply skips it (TKT-6). Claiming
    /// before the spawn (rather than after) keeps this write strictly ahead of
    /// the supervisor's fire-and-forget `done`, so it no longer races
    /// completion the way an unordered post-spawn `in_progress` write would.
    async fn fan_out(
        &self,
        id: &str,
        agents: &HashMap<String, AgentProfile>,
        tiers: &TierRouting,
        repo: &str,
        fe: &ForEachStep,
    ) -> rk_core::Result<Vec<FannedAgent>> {
        let items = self.query_tickets(&fe.query, repo)?;
        if items.is_empty() {
            warn!(instance = %id, "for_each matched no tickets; nothing to fan out");
        }
        // The workflow's own tier rules shadow the global ones for this fan-out.
        let routing = tiers.chained(&self.tier_routing);
        let ctx = self.context(id);
        // The per-instance cap is static for the run; spent is recomputed live
        // in the supervisor per spawn, so later fan-out spawns are refused once
        // earlier ones have burned the instance past its cap.
        let instance_cap = self.instance_budget(id);
        let mut fanned = Vec::with_capacity(items.len());
        for item in items {
            // Atomically claim the ticket before spawning. If a concurrent drain
            // already claimed it, we lose the race and skip it, so one ticket is
            // never dispatched to two rats.
            if !self.tickets.claim(&item.id).await? {
                info!(instance = %id, ticket = %item.id, "ticket already claimed; skipping");
                continue;
            }
            // Route this ticket to a cost tier from its labels/priority. The tier
            // is an agent profile that resolves just below inline overrides.
            let tier = routing.route(&item.labels, Some(&item.priority));
            if let Some(tier) = tier {
                info!(instance = %id, ticket = %item.id, tier, "routed ticket to cost tier");
            }
            let resolved = resolve_fields(
                fe.agent.as_deref(),
                tier,
                fe.harness.as_deref(),
                fe.model.as_deref(),
                fe.permission_mode.as_deref(),
                agents,
                &self.global_agents,
                &self.default_harness,
            )?;
            let title = interpolate_item(&fe.task.title, &item, &ctx);
            let prompt = fe
                .task
                .description
                .as_ref()
                .map(|d| interpolate_item(d, &item, &ctx));
            let record = self.supervisor.spawn(SpawnParams {
                repo: repo.to_string(),
                task: title,
                prompt,
                role: fe.role.clone(),
                harness: Some(resolved.harness),
                parent: None,
                // Each rat gets its own branch off the base; fan-out never
                // chains onto ctx.active_branch (that would serialize them).
                base: fe.branch.clone(),
                model: resolved.model,
                permission_mode: resolved.permission_mode,
                attach: false,
                workflow_instance: Some(id.to_string()),
                instance_max_usd: instance_cap,
            })?;
            fanned.push(FannedAgent {
                agent: record.name.clone(),
                branch: record.branch.clone(),
                ticket: Some(item.id),
            });
        }
        Ok(fanned)
    }

    /// The predicate a `wait`/`wait_all` blocks on: THIS generation of `agent`
    /// reporting its `harness_result`.
    ///
    /// The agent name alone is not enough. `harness_result` events are durable
    /// and outlive the rat they name forever, so a bare `"agent":"<name>"`
    /// search matches a PREDECESSOR of the same name and satisfies the wait in
    /// milliseconds. That is TKT-146: TKT-136 briefly let an archived name be
    /// reused, the wait returned a two-day-old namesake's tuple, the following
    /// `evaluate` judged a stranger's work, and the `dismiss` behind it killed
    /// a rat one second into its task (SIGTERM, so `code None`, no session,
    /// zero tokens). Whole workflows reported success having done nothing.
    ///
    /// `reserve_name` no longer recycles names, so the collision should not
    /// arise — but a `wait` that can be satisfied by a tuple predating the rat
    /// it waits on is wrong on its own terms. Bounding the read below the
    /// agent record's `created_at` makes the predicate generation-exact and
    /// keeps it correct however the naming policy moves.
    ///
    /// TKT-159: the bound is now unconditional. It previously degraded to an
    /// UNBOUNDED read when the agent's registry record was unreachable, which
    /// left the exact defect this method exists to prevent live on that path.
    /// [`generation_floor`](Self::generation_floor) now always yields a valid
    /// bound, so there is no case in which a `wait` can match a namesake.
    fn result_pattern(&self, id: &str, agent: &str) -> Pattern {
        Pattern::for_agent_since(
            Category::Event,
            "harness_result",
            agent,
            self.generation_floor(id, agent),
        )
    }

    /// The instant a waited-on agent's `harness_result` provably postdates.
    ///
    /// The agent record's own `created_at` is the exact answer. When no record
    /// is reachable — it was removed, or the registry file was replaced under a
    /// resumed instance — fall back to when THIS workflow instance started:
    /// every agent a `wait`/`wait_all` blocks on was spawned by this instance
    /// (`ctx.active_agent` is only set by `spawn`, `ctx.fanout` only by
    /// `for_each`), so its result cannot predate the instance. That makes the
    /// fallback a sound lower bound rather than no bound at all — never too
    /// tight to miss the real tuple, and still tight enough to exclude every
    /// namesake predecessor from before the run.
    fn generation_floor(&self, id: &str, agent: &str) -> DateTime<Utc> {
        let record_created_at = self.supervisor.status(agent).map(|r| r.created_at);
        if record_created_at.is_none() {
            warn!(
                agent,
                instance = id,
                "no registry record for waited-on agent; falling back to the instance start"
            );
        }
        let instance_started_at = self.lock().get(id).map(|i| i.started_at);
        generation_floor_of(record_created_at, instance_started_at, Utc::now())
    }

    /// Block until every fanned-out agent has emitted its `harness_result`,
    /// then aggregate into `{count, ok, errors, all_ok, results}`. All agents
    /// share one deadline: the step times out if any is still running when it
    /// elapses.
    async fn join(
        &self,
        id: &str,
        fanout: &[FannedAgent],
        wait_all: &WaitAllStep,
    ) -> rk_core::Result<Value> {
        if fanout.is_empty() {
            return Err(rk_core::Error::other(
                "wait_all step with no fan-out agents (missing or empty for_each)",
            ));
        }
        let deadline = tokio::time::Instant::now() + parse_duration(&wait_all.timeout)?;
        let mut results = Vec::with_capacity(fanout.len());
        for fa in fanout {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            // Same generation-exact predicate as `wait`.
            let pattern = self.result_pattern(id, &fa.agent);
            let tuple = self
                .space
                .rd(&pattern, remaining)
                .await
                .map_err(|e| rk_core::Error::other(format!("wait_all failed: {e}")))?
                .ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "wait_all timed out after {} waiting on agent {}",
                        wait_all.timeout, fa.agent
                    ))
                })?;
            results.push(tuple.payload.clone());
        }
        let ok = results
            .iter()
            .filter(|r| r.get("is_error").and_then(Value::as_bool) == Some(false))
            .count();
        let count = results.len();
        Ok(json!({
            "count": count,
            "ok": ok,
            "errors": count - ok,
            "all_ok": ok == count,
            "results": results,
        }))
    }

    /// Dismiss every agent in the fan-out set in parallel — the fan-out
    /// counterpart to a single `dismiss` over `active_agent`. Each agent is
    /// merged (unless `no_merge`) and cleaned up concurrently, then the caller
    /// clears the fan-out set. Aggregates into `{count, merged, parked, errors,
    /// all_merged, results}`. A hard dismiss failure (e.g. a git error — a
    /// merge *conflict* is a clean `merged: false`, not an error) fails the
    /// step, symmetric to how `wait_all` fails on a timeout.
    ///
    /// When `dismiss_all.only_clean` is set, this reads the preceding
    /// `wait_all` aggregate (`previous_result`) and merges *only* the branches
    /// of rats that finished clean (`is_error: false`), parking every failed
    /// rat's branch with `no_merge` for review instead of failing the whole
    /// batch. A branch parked because its rat failed is counted in `parked`
    /// (distinct from a `merged: false` merge *conflict*), and `all_merged`
    /// stays `merged == count`, so a following `evaluate {all_merged: true}`
    /// still surfaces the failure in `rk inbox` — but only after the clean
    /// branches have already merged. `only_clean` requires a preceding
    /// `wait_all` (its per-agent results supply the clean/failed signal); it
    /// fails the step if none is present rather than silently merging all.
    async fn dismiss_fanout(
        &self,
        fanout: &[FannedAgent],
        dismiss_all: &DismissAllStep,
        previous_result: Option<&Value>,
    ) -> rk_core::Result<Value> {
        if fanout.is_empty() {
            return Err(rk_core::Error::other(
                "dismiss_all step with no fan-out agents (missing or empty for_each)",
            ));
        }
        // With only_clean, the per-agent no_merge is driven by the preceding
        // wait_all's results: an agent is parked (no_merge=true) unless its
        // harness_result reported is_error:false. Without a preceding wait_all
        // there is no clean/failed signal, so the flag is meaningless — fail
        // rather than silently merge everything.
        let clean = if dismiss_all.only_clean {
            let agg = previous_result.ok_or_else(|| {
                rk_core::Error::other(
                    "dismiss_all onlyClean requires a preceding wait_all: no aggregate in \
                     ctx.previous_result to determine which rats finished clean",
                )
            })?;
            let results = agg
                .get("results")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    rk_core::Error::other(
                    "dismiss_all onlyClean requires a preceding wait_all: ctx.previous_result has \
                     no `results` array (is the previous step a wait_all?)",
                )
                })?;
            let clean: std::collections::HashSet<String> = results
                .iter()
                .filter(|r| r.get("is_error").and_then(Value::as_bool) == Some(false))
                .filter_map(|r| r.get("agent").and_then(Value::as_str).map(str::to_string))
                .collect();
            Some(clean)
        } else {
            None
        };
        // Dismiss all branches concurrently: each dismissal kills its child and
        // merges its branch independently, so serializing them would waste the
        // whole point of a fan-out.
        let mut set = tokio::task::JoinSet::new();
        for fa in fanout {
            let supervisor = Arc::clone(&self.supervisor);
            let agent = fa.agent.clone();
            // Base no_merge from the step, plus: under only_clean, park (don't
            // merge) any agent not in the clean set.
            let parked = clean
                .as_ref()
                .is_some_and(|clean| !clean.contains(&fa.agent));
            let no_merge = dismiss_all.no_merge || parked;
            set.spawn(async move {
                let outcome = supervisor.dismiss(&agent, no_merge).await;
                (agent, parked, outcome)
            });
        }
        let count = fanout.len();
        let mut results = Vec::with_capacity(count);
        let mut merged = 0usize;
        let mut parked = 0usize;
        let mut failures = Vec::new();
        while let Some(joined) = set.join_next().await {
            let (agent, was_parked, outcome) = joined
                .map_err(|e| rk_core::Error::other(format!("dismiss_all task join error: {e}")))?;
            match outcome {
                Ok(value) => {
                    if value.get("merged").and_then(Value::as_bool) == Some(true) {
                        merged += 1;
                    } else if was_parked {
                        // Held back because the rat failed, not because the
                        // branch would not merge — track it separately so a
                        // following evaluate/report can tell the two apart.
                        parked += 1;
                    }
                    results.push(value);
                }
                Err(e) => {
                    failures.push(format!("{agent}: {e}"));
                    results.push(json!({"agent": agent, "error": e.to_string()}));
                }
            }
        }
        if !failures.is_empty() {
            return Err(rk_core::Error::other(format!(
                "dismiss_all failed for {} of {count} agents: {}",
                failures.len(),
                failures.join("; ")
            )));
        }
        Ok(json!({
            "count": count,
            "merged": merged,
            "parked": parked,
            "errors": count - merged,
            "all_merged": merged == count,
            "results": results,
        }))
    }

    /// Execute a `run` step's command in the active agent's worktree — the
    /// deterministic quality gate. Where `evaluate` unifies only against the
    /// harness's self-reported output (it takes the rat's word), this runs the
    /// repo's real checks and captures `{exit, stdout, stderr}` into a value
    /// for `ctx.previous_result`, so a following `evaluate {expect: {exit: 0}}`
    /// (or a `when`) gates the merge on a verdict the runner cannot forge.
    ///
    /// Fail-closed: a spawn failure, a timeout (the child is killed on drop),
    /// or an `expect_exit` mismatch all return an `Err` that fails the instance
    /// rather than letting a red — or hung — suite slip through.
    async fn run_command(
        &self,
        ctx: &WorkflowContext,
        run: &RunStep,
        repo: &str,
    ) -> rk_core::Result<Value> {
        let agent = ctx
            .active_agent
            .clone()
            .ok_or_else(|| rk_core::Error::other("run step with no active agent"))?;
        let record = self.supervisor.status(&agent).ok_or_else(|| {
            rk_core::Error::other(format!("run step: no record for agent {agent}"))
        })?;
        let worktree = record.worktree.ok_or_else(|| {
            rk_core::Error::other(format!("run step: agent {agent} has no worktree"))
        })?;
        // Resolve the effective command, cwd, expect_exit, and timeout from
        // either a repo-registered named check or a raw inline command — the
        // latter gated fail-closed by the require_named_checks policy (TKT-30).
        let resolved = self.resolve_run(run, repo)?;
        // Resolve cwd relative to the worktree root; interpolate ctx
        // placeholders in both fields for parity with the other steps.
        let mut dir = worktree.clone();
        if let Some(sub) = &resolved.cwd {
            dir = dir.join(interpolate(sub, ctx));
        }
        let command = interpolate(&resolved.command, ctx);
        let timeout = parse_duration(&resolved.timeout)?;

        let child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .current_dir(&dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // Kill the suite if the timeout below drops the wait future, so a
            // hung check leaves no orphan behind.
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                rk_core::Error::other(format!("run step: failed to spawn `{command}`: {e}"))
            })?;

        let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(res) => res
                .map_err(|e| rk_core::Error::other(format!("run step: `{command}` failed: {e}")))?,
            Err(_) => {
                // Fail closed: a suite that outruns its timeout is a red gate.
                return Err(rk_core::Error::other(format!(
                    "run step: `{command}` timed out after {}",
                    resolved.timeout
                )));
            }
        };

        let exit = output.status.code().unwrap_or(-1) as i64;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        info!(agent = %agent, exit, command = %command, "run step completed");
        let result = json!({"exit": exit, "stdout": stdout, "stderr": stderr});

        // Inline fail-closed gate: when the step (or named check) declares the
        // expected exit, enforce it here so `run` can gate on its own without a
        // trailing evaluate. When unset, the exit is left for a following
        // evaluate/when.
        if let Some(expected) = resolved.expect_exit {
            if exit != expected {
                return Err(rk_core::Error::other(format!(
                    "run step: `{command}` exited {exit}, expected {expected}"
                )));
            }
        }
        Ok(result)
    }

    /// Resolve a `run` step to its effective command, cwd, exit gate, and
    /// timeout — enforcing the named-check policy (TKT-30).
    ///
    /// A step names EITHER a raw `command` OR a repo-registered `check`, never
    /// both and never neither. A `check` is looked up in `<repo>/.rk/checks.cue`
    /// (the repo owner's allowlist); its command/cwd/expectExit/timeout supply
    /// the defaults, with the step's own `cwd`/`expectExit`/`timeout` (when set)
    /// taking precedence. A raw `command` is refused fail-closed when the
    /// `require_named_checks` policy is on, so a compromised workflow definition
    /// cannot run arbitrary shell — only the checks the repo registered.
    fn resolve_run(&self, run: &RunStep, repo: &str) -> rk_core::Result<ResolvedRun> {
        match (&run.command, &run.check) {
            (Some(_), Some(_)) => Err(rk_core::Error::other(
                "run step: set exactly one of `command` or `check`, not both",
            )),
            (None, None) => Err(rk_core::Error::other(
                "run step: set one of `command` (raw) or `check` (named)",
            )),
            (Some(command), None) => {
                if self.require_named_checks {
                    return Err(rk_core::Error::other(
                        "run step: raw `command` refused by policy (require_named_checks); \
                         reference a named `check` registered in <repo>/.rk/checks.cue",
                    ));
                }
                Ok(ResolvedRun {
                    command: command.clone(),
                    cwd: run.cwd.clone(),
                    expect_exit: run.expect_exit,
                    timeout: run.timeout.clone(),
                })
            }
            (None, Some(name)) => {
                let check = self.find_check(repo, name)?;
                // Step-level overrides win over the check's own defaults; the
                // step's timeout only overrides when it is non-default (a check
                // gets to set its own bound without every referencing step
                // having to restate it).
                let timeout = if run.timeout == DEFAULT_RUN_TIMEOUT {
                    check
                        .timeout
                        .clone()
                        .unwrap_or_else(|| DEFAULT_RUN_TIMEOUT.to_string())
                } else {
                    run.timeout.clone()
                };
                Ok(ResolvedRun {
                    command: check.command,
                    cwd: run.cwd.clone().or(check.cwd),
                    expect_exit: run.expect_exit.or(check.expect_exit),
                    timeout,
                })
            }
        }
    }

    /// Look up a named check in the repo's registry (`<repo>/.rk/checks.cue`).
    /// Fails closed: a missing registry, an unparseable one, or an unknown name
    /// all error rather than silently running nothing.
    fn find_check(&self, repo: &str, name: &str) -> rk_core::Result<rk_workflow::Check> {
        let file = std::path::PathBuf::from(repo)
            .join(".rk")
            .join("checks.cue");
        if !file.exists() {
            return Err(rk_core::Error::other(format!(
                "run step: check '{name}' referenced but no registry at {}",
                file.display()
            )));
        }
        let checks = rk_workflow::load_checks(&file)?;
        checks.into_iter().find(|c| c.name == name).ok_or_else(|| {
            rk_core::Error::other(format!(
                "run step: no check named '{name}' in {}",
                file.display()
            ))
        })
    }

    /// Resolve a fan-out ticket query to a bounded list of items in the
    /// workflow's own repo scope. `status: "ready"` uses dependency-aware
    /// readiness; any other value is a literal status filter.
    fn query_tickets(&self, query: &TicketQuery, repo: &str) -> rk_core::Result<Vec<TicketItem>> {
        let scope = Some(repo_name_of(repo));
        let tuples = if query.status == "ready" {
            self.tickets.ready(scope)?
        } else {
            self.tickets.list(scope, Some(query.status.clone()), None)?
        };
        Ok(tuples
            .into_iter()
            .take(query.limit)
            .map(|t| TicketItem {
                id: t.identity.clone(),
                title: field(&t.payload, "title"),
                body: field(&t.payload, "body"),
                priority: field(&t.payload, "priority"),
                labels: string_array(&t.payload, "labels"),
            })
            .collect())
    }

    pub fn list(&self) -> Vec<Instance> {
        let mut all: Vec<Instance> = self.lock().values().cloned().collect();
        all.sort_by_key(|i| i.started_at);
        all
    }

    pub fn status(&self, id: &str) -> Option<Instance> {
        self.lock().get(id).cloned()
    }

    /// The instance plus its labelled step trace, for `rk workflow timeline`:
    /// every step of the definition rendered as a row so the CLI can mark
    /// done/current/pending against the persisted `current_step` cursor.
    /// `None` rows = the definition no longer loads (file moved or deleted
    /// since launch); the CLI then falls back to bare step numbers.
    pub fn timeline(&self, id: &str) -> Option<(Instance, Option<Vec<TimelineRow>>)> {
        let instance = self.status(id)?;
        let rows = self
            .find_definition(&instance.definition, &instance.repo)
            .ok()
            .and_then(|file| rk_workflow::load(&file, &instance.params).ok())
            .map(|workflow| timeline_rows(&workflow.steps));
        Some((instance, rows))
    }

    /// Record a human approval decision for a parked instance. Writes a
    /// `workflow_approval` event that an approval gate blocked on this instance
    /// is waiting to read. Idempotent from the caller's view: the first
    /// decision to reach the blocked gate wins.
    pub fn approve(
        &self,
        instance_id: &str,
        approved: bool,
        by: &str,
        reason: Option<String>,
    ) -> rk_core::Result<()> {
        let instance = self.status(instance_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such workflow instance: {instance_id}"))
        })?;
        let payload = json!({
            "instance": instance_id,
            "step": instance.current_step,
            "approved": approved,
            "by": by,
            "reason": reason,
        });
        self.space.out(rk_core::tuple::Tuple::new(
            Category::Event,
            repo_name_of(&instance.repo),
            "workflow_approval",
            by.to_string(),
            payload,
        ))?;
        info!(instance = %instance_id, approved, by = %by, "workflow approval recorded");
        Ok(())
    }

    fn context(&self, id: &str) -> WorkflowContext {
        self.lock()
            .get(id)
            .map(|i| i.context.clone())
            .unwrap_or_default()
    }

    /// This instance's per-run budget cap (from the workflow's `budget:`), used
    /// as the dispatch preflight ceiling on every spawn it makes.
    fn instance_budget(&self, id: &str) -> Option<f64> {
        self.lock().get(id).and_then(|i| i.instance_max_usd)
    }

    fn store(&self, instance: Instance) {
        self.lock().insert(instance.id.clone(), instance.clone());
        self.persist(&instance);
    }

    fn update<F: FnOnce(&mut Instance)>(&self, id: &str, mutate: F) {
        let mut instances = self.lock();
        if let Some(instance) = instances.get_mut(id) {
            mutate(instance);
            let snapshot = instance.clone();
            drop(instances);
            self.persist(&snapshot);
        }
    }

    fn persist(&self, instance: &Instance) {
        let dir = self.layout.home().join("workflow-instances");
        if std::fs::create_dir_all(&dir).is_err() {
            return;
        }
        let path = dir.join(format!("{}.json", instance.id));
        if let Ok(data) = serde_json::to_vec_pretty(instance) {
            if let Err(e) = std::fs::write(&path, data) {
                warn!(error = %e, "failed to persist workflow instance");
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Instance>> {
        match self.instances.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
}

/// A ticket flattened into the fields a fan-out task template can bind, plus the
/// `labels`/`priority` a tier-routing rule keys on.
struct TicketItem {
    id: String,
    title: String,
    body: String,
    priority: String,
    labels: Vec<String>,
}

fn field(payload: &Value, key: &str) -> String {
    payload
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn string_array(payload: &Value, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Interpolate a fan-out task template: `{{ctx.*}}` first, then the per-ticket
/// `{{item.id}}` / `{{item.title}}` / `{{item.body}}` placeholders.
fn interpolate_item(text: &str, item: &TicketItem, ctx: &WorkflowContext) -> String {
    interpolate(text, ctx)
        .replace("{{item.id}}", &item.id)
        .replace("{{item.title}}", &item.title)
        .replace("{{item.body}}", &item.body)
}

/// Replace `{{ctx.*}}` placeholders in workflow strings at execution time.
fn interpolate(text: &str, ctx: &WorkflowContext) -> String {
    let previous = ctx
        .previous_result
        .as_ref()
        .map(|v| {
            v["result"]
                .as_str()
                .map(String::from)
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default();
    let mut out = text
        .replace(
            "{{ctx.activeAgent}}",
            ctx.active_agent.as_deref().unwrap_or(""),
        )
        .replace(
            "{{ctx.activeBranch}}",
            ctx.active_branch.as_deref().unwrap_or(""),
        )
        .replace("{{ctx.previousResult}}", &previous);
    // `read`-lifted variables: {{ctx.var.<name>}}.
    for (name, value) in &ctx.vars {
        out = out.replace(&format!("{{{{ctx.var.{name}}}}}"), &value_as_key(value));
    }
    out
}

/// Render a ctx variable as a plain string for `when`-case matching and
/// interpolation: strings pass through, null becomes empty, anything else is
/// its compact JSON form.
fn value_as_key(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Pick the lower bound for a generation-exact `harness_result` read, given
/// what is still known about the waited-on agent. Pure so the choice is
/// testable without a live supervisor; see
/// [`WorkflowEngine::generation_floor`].
///
/// Ordered by how tight a bound each source gives, and every arm returns SOME
/// instant — there is deliberately no "unbounded" result. That is the TKT-159
/// fix: this decision used to fall through to no bound at all when the agent
/// record was missing, which reinstated the TKT-146 defect (a `wait` satisfied
/// by a namesake predecessor's two-day-old tuple) on exactly that path.
fn generation_floor_of(
    record_created_at: Option<DateTime<Utc>>,
    instance_started_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    // 1. The agent record's own birth: exact, and the normal case.
    // 2. The instance's start: every waited-on agent was spawned by this
    //    instance, so its result cannot predate the run. Looser but sound.
    // 3. `now`: nothing is known. Cannot admit an older namesake's tuple, which
    //    is the failure mode that kills a live rat. The cost is a wait that
    //    times out if the result already landed — fail toward waiting, never
    //    toward a stranger's record.
    record_created_at.or(instance_started_at).unwrap_or(now)
}

fn repo_name_of(repo: &str) -> String {
    PathBuf::from(repo)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "repo".into())
}

fn parse_duration(s: &str) -> rk_core::Result<Duration> {
    let s = s.trim();
    let invalid = || rk_core::Error::other(format!("invalid duration: {s}"));
    // Split on the last *char*, not the last byte: a multibyte suffix (e.g.
    // "5m²", "10µ") would make byte-index split_at panic on a non-boundary.
    // The unit chars (s/m/h) are single-byte ASCII, so trimming one byte off
    // the end when they match is always a valid boundary.
    let (value, mult) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1u64),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        _ => (s, 1),
    };
    let n = value.parse::<u64>().map_err(|_| invalid())?;
    // checked_mul: a huge value like "9223372036854775807m" would otherwise
    // panic in debug builds and silently wrap in release.
    n.checked_mul(mult)
        .map(Duration::from_secs)
        .ok_or_else(invalid)
}

/// One row of an instance's rendered step trace. `index` is the TOP-LEVEL
/// step index the row belongs to — the executor's `current_step` cursor only
/// counts top-level steps, so nested rows (a `when` case body, a `repeat`
/// body) carry their parent's index and a deeper `depth` for indentation.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineRow {
    pub index: usize,
    pub depth: usize,
    pub label: String,
}

/// Flatten a workflow's steps into labelled timeline rows, recursing into
/// `when`/`repeat` bodies with increased depth.
fn timeline_rows(steps: &[Step]) -> Vec<TimelineRow> {
    let mut rows = Vec::new();
    for (index, step) in steps.iter().enumerate() {
        flatten_step(&mut rows, index, 0, step);
    }
    rows
}

fn flatten_step(rows: &mut Vec<TimelineRow>, index: usize, depth: usize, step: &Step) {
    rows.push(TimelineRow {
        index,
        depth,
        label: step_label(step),
    });
    match step {
        Step::When(when) => {
            // HashMap order is nondeterministic; sort so the trace is stable.
            let mut cases: Vec<_> = when.cases.iter().collect();
            cases.sort_by(|a, b| a.0.cmp(b.0));
            for (value, body) in cases {
                rows.push(TimelineRow {
                    index,
                    depth: depth + 1,
                    label: format!("case {value}:"),
                });
                for s in body {
                    flatten_step(rows, index, depth + 2, s);
                }
            }
            if !when.default.is_empty() {
                rows.push(TimelineRow {
                    index,
                    depth: depth + 1,
                    label: "default:".into(),
                });
                for s in &when.default {
                    flatten_step(rows, index, depth + 2, s);
                }
            }
        }
        Step::Repeat(repeat) => {
            for s in &repeat.steps {
                flatten_step(rows, index, depth + 1, s);
            }
        }
        _ => {}
    }
}

/// Short human label for one step, mirroring the CUE field names an operator
/// wrote in the definition.
fn step_label(step: &Step) -> String {
    match step {
        Step::Spawn(s) => format!("spawn {} — \"{}\"", s.role, s.task.title),
        Step::Wait(w) => format!("wait for result ({})", w.timeout),
        Step::Evaluate(e) => {
            if e.any_of.is_empty() {
                format!("evaluate expect {}", e.expect)
            } else {
                format!("evaluate expect {} (+{} anyOf)", e.expect, e.any_of.len())
            }
        }
        Step::Dismiss(d) => {
            if d.no_merge {
                "dismiss (no merge)".into()
            } else {
                "dismiss (merge)".into()
            }
        }
        Step::Gate(g) => match (&g.duration, &g.timeout) {
            (Some(d), _) => format!("gate {} ({d})", g.gate_type),
            (None, Some(t)) => format!("gate {} (timeout {t})", g.gate_type),
            (None, None) => format!("gate {}", g.gate_type),
        },
        Step::Read(r) => {
            let field = r
                .field
                .as_deref()
                .map(|f| format!(".{f}"))
                .unwrap_or_default();
            format!("read {}/{}{} → {}", r.category, r.identity, field, r.into)
        }
        Step::When(w) => format!("when {}", w.var),
        Step::Repeat(r) => format!("repeat ×{}", r.max),
        Step::Break => "break".into(),
        Step::Stop(s) => match &s.reason {
            Some(reason) => format!("stop — {reason}"),
            None => "stop".into(),
        },
        Step::ForEach(f) => format!(
            "for_each {} tickets (≤{}) → spawn {}",
            f.query.status, f.query.limit, f.role
        ),
        Step::WaitAll(w) => format!("wait_all ({})", w.timeout),
        Step::DismissAll(d) => {
            let mut label = String::from("dismiss_all");
            if d.no_merge {
                label.push_str(" (no merge)");
            } else if d.only_clean {
                label.push_str(" (only clean)");
            }
            label
        }
        Step::Run(r) => match (&r.check, &r.command) {
            (Some(check), _) => format!("run check:{check}"),
            (None, Some(command)) => format!("run `{command}`"),
            (None, None) => "run".into(),
        },
        Step::Land(l) => format!("land {} → {}", l.branch, l.target),
        Step::OpenPr(p) => format!("open_pr {} → {}", p.branch, p.target),
        Step::SubWorkflow(s) => format!("sub_workflow {}", s.workflow),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::id::RecordId;

    #[test]
    fn interpolate_replaces_ctx_placeholders() {
        let ctx = WorkflowContext {
            active_agent: Some("Whisker".into()),
            active_branch: Some("rat/whisker/t1".into()),
            previous_result: Some(json!({"result": "looks good", "is_error": false})),
            ..Default::default()
        };
        let text = "Review {{ctx.activeBranch}} by {{ctx.activeAgent}}: {{ctx.previousResult}}";
        assert_eq!(
            interpolate(text, &ctx),
            "Review rat/whisker/t1 by Whisker: looks good"
        );
    }

    #[test]
    fn interpolate_replaces_read_vars() {
        let ctx = WorkflowContext {
            vars: HashMap::from([
                ("verdict".to_string(), json!("REWORK")),
                ("rounds".to_string(), json!(3)),
            ]),
            ..Default::default()
        };
        let text = "verdict={{ctx.var.verdict}} rounds={{ctx.var.rounds}}";
        assert_eq!(interpolate(text, &ctx), "verdict=REWORK rounds=3");
    }

    /// TKT-159 regression. Reverting the fallback — i.e. leaving the wait
    /// unbounded when the agent record is gone — makes this read admit a
    /// namesake predecessor's `harness_result` again, which is the TKT-146
    /// kill-a-live-rat defect. Every arm must yield a floor that excludes any
    /// tuple written before the run started.
    #[test]
    fn generation_floor_is_never_unbounded() {
        let started_at = Utc::now();
        let spawned_at = started_at + chrono::Duration::seconds(5);
        // A namesake's durable tuple from a previous night — the input that made
        // TKT-146 fire, and which the 24 duplicated name generations supply today.
        let predecessor = RecordId::floor_at(started_at - chrono::Duration::days(2));

        // Record present: the exact, tightest bound.
        assert_eq!(
            generation_floor_of(Some(spawned_at), Some(started_at), Utc::now()),
            spawned_at,
        );
        // Record gone: fall back to the instance start, which the agent this
        // instance spawned provably postdates.
        assert_eq!(
            generation_floor_of(None, Some(started_at), Utc::now()),
            started_at,
        );
        // Neither survives: `now`, the most conservative bound.
        let now = Utc::now();
        assert_eq!(generation_floor_of(None, None, now), now);

        // The property that actually matters on every arm.
        for floor in [
            generation_floor_of(Some(spawned_at), Some(started_at), now),
            generation_floor_of(None, Some(started_at), now),
            generation_floor_of(None, None, now),
        ] {
            assert!(
                predecessor <= RecordId::floor_at(floor),
                "floor {floor} would admit a predecessor's tuple",
            );
        }
    }

    /// The fallback must never be so tight that it misses the tuple the wait is
    /// actually for: a result written after the instance started still matches.
    #[test]
    fn instance_start_fallback_still_admits_this_generations_result() {
        let started_at = Utc::now();
        let floor = generation_floor_of(None, Some(started_at), Utc::now());
        let pattern = Pattern::for_agent_since(Category::Event, "harness_result", "Whisker", floor);
        let mut mine = rk_core::tuple::Tuple::new(
            Category::Event,
            "myrepo",
            "harness_result",
            "castle",
            json!({"agent": "Whisker", "is_error": false}),
        );
        mine.id = RecordId::floor_at(started_at + chrono::Duration::seconds(90));
        assert!(pattern.matches(&mine));
    }

    #[test]
    fn value_as_key_renders_variants() {
        assert_eq!(value_as_key(&json!("APPROVE")), "APPROVE");
        assert_eq!(value_as_key(&Value::Null), "");
        assert_eq!(value_as_key(&json!(42)), "42");
    }

    #[test]
    fn interpolate_item_binds_ticket_fields() {
        let ctx = WorkflowContext::default();
        let item = TicketItem {
            id: "TKT-7".into(),
            title: "add caching".into(),
            body: "cache the API layer".into(),
            priority: "normal".into(),
            labels: vec![],
        };
        let text = "Work {{item.id}}: {{item.title}}\n\n{{item.body}}";
        assert_eq!(
            interpolate_item(text, &item, &ctx),
            "Work TKT-7: add caching\n\ncache the API layer"
        );
    }

    #[test]
    fn parse_duration_handles_units_and_bare_numbers() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        // Bare number with no unit is treated as seconds.
        assert_eq!(parse_duration("45").unwrap(), Duration::from_secs(45));
        // Surrounding whitespace is trimmed.
        assert_eq!(parse_duration("  10m ").unwrap(), Duration::from_secs(600));
    }

    #[test]
    fn parse_duration_rejects_multibyte_suffix_without_panicking() {
        // Non-boundary byte split used to panic here; must return Err instead.
        assert!(parse_duration("5m²").is_err());
        assert!(parse_duration("10µ").is_err());
        assert!(parse_duration("²").is_err());
    }

    #[test]
    fn parse_duration_rejects_overflow() {
        // u64::MAX minutes would overflow the seconds multiplication.
        assert!(parse_duration("9223372036854775807m").is_err());
        assert!(parse_duration("18446744073709551615h").is_err());
    }

    #[test]
    fn parse_duration_rejects_empty_and_garbage() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("   ").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("m").is_err());
    }

    #[test]
    fn timeline_rows_flatten_and_label_steps() {
        let steps: Vec<Step> = serde_json::from_value(serde_json::json!([
            {"type": "spawn", "task": {"title": "fix the bug"}},
            {"type": "wait", "timeout": "30m"},
            {"type": "gate", "gateType": "approval", "timeout": "24h"},
            {"type": "read", "category": "event", "identity": "workflow_approval",
             "field": "approved", "into": "verdict"},
            {"type": "when", "var": "verdict",
             "cases": {"true": [{"type": "dismiss"}]},
             "default": [{"type": "dismiss", "noMerge": true}, {"type": "stop", "reason": "rejected"}]},
        ]))
        .unwrap();

        let rows = timeline_rows(&steps);
        let rendered: Vec<(usize, usize, &str)> = rows
            .iter()
            .map(|r| (r.index, r.depth, r.label.as_str()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                (0, 0, "spawn rat — \"fix the bug\""),
                (1, 0, "wait for result (30m)"),
                (2, 0, "gate approval (timeout 24h)"),
                (3, 0, "read event/workflow_approval.approved → verdict"),
                (4, 0, "when verdict"),
                (4, 1, "case true:"),
                (4, 2, "dismiss (merge)"),
                (4, 1, "default:"),
                (4, 2, "dismiss (no merge)"),
                (4, 2, "stop — rejected"),
            ]
        );
    }

    #[test]
    fn timeline_rows_nest_repeat_bodies() {
        let steps: Vec<Step> = serde_json::from_value(serde_json::json!([
            {"type": "repeat", "max": 3, "steps": [
                {"type": "run", "command": "cargo test"},
                {"type": "break"},
            ]},
            {"type": "land", "branch": "{{ctx.activeBranch}}", "target": "main"},
        ]))
        .unwrap();

        let rows = timeline_rows(&steps);
        let rendered: Vec<(usize, usize, &str)> = rows
            .iter()
            .map(|r| (r.index, r.depth, r.label.as_str()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                (0, 0, "repeat ×3"),
                (0, 1, "run `cargo test`"),
                (0, 1, "break"),
                (1, 0, "land {{ctx.activeBranch}} → main"),
            ]
        );
    }
}
