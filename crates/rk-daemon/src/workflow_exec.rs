//! Workflow execution: sequential step machine over the supervisor and the
//! tuplespace. Definitions come from rk-workflow (cue CLI); this module owns
//! instances, context threading, and step semantics.

use crate::supervisor::{SpawnParams, Supervisor};
use crate::tickets::Tickets;
use rk_core::id::prefixed_id;
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Pattern};
use rk_space::Space;
use rk_workflow::{
    resolve::{resolve, resolve_fields},
    AgentProfile, ForEachStep, Step, TicketQuery, WaitAllStep, Workflow,
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
    default_harness: String,
    instances: Mutex<HashMap<String, Instance>>,
}

impl WorkflowEngine {
    pub fn new(
        layout: Layout,
        supervisor: Arc<Supervisor>,
        space: Space,
        tickets: Arc<Tickets>,
        global_agents: HashMap<String, AgentProfile>,
        default_harness: String,
    ) -> Self {
        Self {
            layout,
            supervisor,
            space,
            tickets,
            global_agents,
            default_harness,
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
            started_at: chrono::Utc::now(),
            completed_at: None,
        };
        self.store(instance.clone());

        let engine = Arc::clone(self);
        let snapshot = instance.clone();
        tokio::spawn(async move {
            let id = snapshot.id.clone();
            let result = engine.execute(&id, workflow, &snapshot.repo).await;
            let (status, error) = match result {
                Ok(()) => (InstanceStatus::Completed, None),
                Err(e) => (InstanceStatus::Failed, Some(e.to_string())),
            };
            engine.update(&id, |i| {
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
            let _ = engine.space.out(rk_core::tuple::Tuple::new(
                Category::Event,
                repo_name_of(&snapshot.repo),
                final_status,
                "daemon".to_string(),
                json!({"instance": id, "workflow": snapshot.workflow, "error": error}),
            ));
        });
        Ok(instance)
    }

    /// Run the top-level step list once. `current_step` tracks top-level
    /// progress only; steps nested inside `when`/`repeat` execute in place
    /// without advancing it (they are bounded by the `repeat` cap).
    async fn execute(&self, id: &str, workflow: Workflow, repo: &str) -> rk_core::Result<()> {
        for (index, step) in workflow.steps.iter().enumerate() {
            self.update(id, |i| i.current_step = index);
            if let Flow::Break = self.run_step(id, step, repo, &workflow.agents).await? {
                // A top-level break ends the workflow (nothing to loop out of).
                break;
            }
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
    ) -> StepFuture<'a> {
        Box::pin(async move {
            for step in steps {
                if let Flow::Break = self.run_step(id, step, repo, agents).await? {
                    return Ok(Flow::Break);
                }
            }
            Ok(Flow::Next)
        })
    }

    /// Execute a single step (recursing for `when`/`repeat`).
    fn run_step<'a>(
        &'a self,
        id: &'a str,
        step: &'a Step,
        repo: &'a str,
        agents: &'a HashMap<String, AgentProfile>,
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
                    let mut pattern = Pattern::category(Category::Event).identity("harness_result");
                    // The single authoritative predicate includes this search;
                    // serde_json serializes maps in key order, so the agent
                    // field renders exactly like this.
                    pattern.payload_search = Some(format!("\"agent\":\"{agent}\""));
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
                    let passed = rk_workflow::unify_concrete(&eval.expect, &actual)?;
                    if !passed {
                        return Err(rk_core::Error::other(format!(
                            "evaluate failed: expect {} did not unify with {}",
                            eval.expect, actual
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
                        let duration = gate.duration.as_deref().ok_or_else(|| {
                            rk_core::Error::other("timer gate missing duration")
                        })?;
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
                        let decision = match self
                            .space
                            .rd(&pattern, timeout)
                            .await
                            .map_err(|e| {
                                rk_core::Error::other(format!("approval gate failed: {e}"))
                            })? {
                            Some(tuple) => tuple.payload,
                            // Fail closed: no human response means no merge.
                            None => json!({
                                "approved": false,
                                "reason": format!("no approval within {}", gate.timeout.as_deref().unwrap_or("24h")),
                            }),
                        };
                        self.update(id, |i| {
                            i.context.previous_result = Some(decision);
                        });
                    }
                    other => {
                        return Err(rk_core::Error::other(format!(
                            "unknown gate type: {other}"
                        )));
                    }
                },
                Step::Read(read) => {
                    let category = Category::from_str(&read.category)?;
                    let scope = read.scope.clone().unwrap_or_else(|| repo_name_of(repo));
                    let mut pattern = Pattern::category(category)
                        .scope(scope)
                        .identity(read.identity.clone());
                    pattern.payload_search = read.search.clone();
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
                        rk_core::Error::other(format!(
                            "read timed out after {} for {} tuple '{}'",
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
                    return self.run_steps(id, branch, repo, agents).await;
                }
                Step::Repeat(repeat) => {
                    for _ in 0..repeat.max {
                        if let Flow::Break = self.run_steps(id, &repeat.steps, repo, agents).await?
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
                    let fanout = self.fan_out(id, agents, repo, fe).await?;
                    self.update(id, |i| i.context.fanout = fanout);
                }
                Step::WaitAll(wait_all) => {
                    let summary = self.join(&ctx.fanout, wait_all).await?;
                    self.update(id, |i| i.context.previous_result = Some(summary.clone()));
                }
            }
            Ok(Flow::Next)
        })
    }

    /// Enumerate the matching tickets and spawn one agent per ticket in
    /// parallel, returning the fan-out set. The task title defaults to the
    /// ticket id, so the supervisor owns each ticket's status lifecycle exactly
    /// as it does for any ticket-dispatched rat (→ `done` on a clean finish,
    /// → `closed` on merge). The fan-out deliberately does not pre-claim the
    /// tickets: writing `in_progress` here would race the supervisor's
    /// fire-and-forget `done` write on fast completions. Guarding against
    /// concurrent drains needs an atomic open→in_progress claim (see TKT-6).
    async fn fan_out(
        &self,
        id: &str,
        agents: &HashMap<String, AgentProfile>,
        repo: &str,
        fe: &ForEachStep,
    ) -> rk_core::Result<Vec<FannedAgent>> {
        let items = self.query_tickets(&fe.query, repo)?;
        if items.is_empty() {
            warn!(instance = %id, "for_each matched no tickets; nothing to fan out");
        }
        let ctx = self.context(id);
        let mut fanned = Vec::with_capacity(items.len());
        for item in items {
            let resolved = resolve_fields(
                fe.agent.as_deref(),
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
            })?;
            fanned.push(FannedAgent {
                agent: record.name.clone(),
                branch: record.branch.clone(),
                ticket: Some(item.id),
            });
        }
        Ok(fanned)
    }

    /// Block until every fanned-out agent has emitted its `harness_result`,
    /// then aggregate into `{count, ok, errors, all_ok, results}`. All agents
    /// share one deadline: the step times out if any is still running when it
    /// elapses.
    async fn join(
        &self,
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
            let mut pattern = Pattern::category(Category::Event).identity("harness_result");
            // Same authoritative predicate as `wait`: serde renders the agent
            // field first, so this substring matches exactly one agent.
            pattern.payload_search = Some(format!("\"agent\":\"{}\"", fa.agent));
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

/// A ticket flattened into the fields a fan-out task template can bind.
struct TicketItem {
    id: String,
    title: String,
    body: String,
}

fn field(payload: &Value, key: &str) -> String {
    payload
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
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

fn repo_name_of(repo: &str) -> String {
    PathBuf::from(repo)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "repo".into())
}

fn parse_duration(s: &str) -> rk_core::Result<Duration> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let (value, mult) = match unit {
        "s" => (num, 1u64),
        "m" => (num, 60),
        "h" => (num, 3600),
        _ => (s, 1),
    };
    value
        .parse::<u64>()
        .map(|n| Duration::from_secs(n * mult))
        .map_err(|_| rk_core::Error::other(format!("invalid duration: {s}")))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        let text = "Work {{item.id}}: {{item.title}}\n\n{{item.body}}";
        assert_eq!(
            interpolate_item(text, &item, &ctx),
            "Work TKT-7: add caching\n\ncache the API layer"
        );
    }
}
