//! Cross-ledger convergence: a read-only comparison of the durable views a
//! repository's work is scattered across — tickets, agents, landing events,
//! and git's own history — surfacing contradictions no single view can see
//! on its own.
//!
//! This module never mutates anything. It answers "why is this repo not
//! converging" the way `rk inbox` (`crate::inbox`) answers "what needs a
//! human right now": a pure `build()` function over already-scanned data,
//! unit-testable without a daemon, with the actual tuple scans and git
//! subprocess calls done by the caller (`server.rs::reconcile_value`).
//!
//! Five contradiction families, chosen because each is a gap between two
//! views that nothing else in the fleet reconciles automatically:
//!
//! - [`kind::DELIVERED_BUT_OPEN`] — a ticket's own delivery record disagrees
//!   with its own status field (TKT-18/46/147's mirror image).
//! - [`kind::TERMINAL_ASSIGNEE_ACTIVE_WORK`] — the ticket view says a rat
//!   still owns active work, but the agent view says that rat settled into a
//!   terminal state and nothing picked the work back up.
//! - [`kind::CONFLICT_HELD_LANDING`] — the landing view recorded a `land`
//!   that neither merged nor opened a review, and git confirms the branch
//!   never reached its target by any other route (TKT-171, re-derived
//!   independently of `rk inbox`'s own copy of this check).
//! - [`kind::TRACKER_CONTRADICTS_GIT`] — the ticket view claims a specific
//!   merge commit landed on a specific target, and git's own ancestry check
//!   disagrees.
//! - [`kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE`] — the durable
//!   workflow-instance ledger (`WorkflowEngine::list_all`) records a run as
//!   settled (`Completed`/`Failed`), but the agent view shows that run's own
//!   `active_agent` still live — the engine's own settlement should have
//!   dismissed it, so a live agent past that point is a supervision leak
//!   nothing else in the fleet surfaces.
//!
//! Every violation names a stable identity (`kind:subject`, unchanged across
//! repeated reads of unchanged state), the evidence it was read from, and a
//! suggested [`Authority`] — who can safely act on it without a human in the
//! loop.

use crate::agents::AgentRecord;
use crate::workflow_exec::{Instance, InstanceStatus};
use rk_core::tuple::Tuple;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub mod kind {
    pub const DELIVERED_BUT_OPEN: &str = "delivered-but-open";
    pub const TERMINAL_ASSIGNEE_ACTIVE_WORK: &str = "terminal-assignee-active-work";
    pub const CONFLICT_HELD_LANDING: &str = "conflict-held-landing";
    pub const TRACKER_CONTRADICTS_GIT: &str = "tracker-contradicts-git-history";
    pub const WORKFLOW_SETTLED_AGENT_STILL_LIVE: &str = "workflow-settled-agent-still-live";
}

/// Who can safely act on a violation without a human in the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Authority {
    /// The durable record already proves what the fix is; a script can apply
    /// it with no judgment call.
    Mechanical,
    /// Resolving this means a dispatch decision (redispatch, retry, wait for
    /// an in-flight hand-off) that only the orchestrator has the fleet-wide
    /// context to make safely.
    Orchestrator,
    /// Resolving this means judging intent — a real merge conflict, or a
    /// history that no longer matches what the tracker claims — which is not
    /// safe to automate.
    Human,
}

impl Authority {
    /// Conservatism rank: lower is more autonomous. `crate::authority`'s
    /// policy overrides may only move a violation kind's rank UP (toward
    /// [`Authority::Human`]), never down — the one mechanism this ladder
    /// uses to guarantee a castle policy edit can narrow authority but never
    /// widen it past what the code itself allows.
    pub fn rank(self) -> u8 {
        match self {
            Authority::Mechanical => 0,
            Authority::Orchestrator => 1,
            Authority::Human => 2,
        }
    }

    /// Parse the `snake_case` wire spelling this type serializes as
    /// (`"mechanical" | "orchestrator" | "human"`), the same vocabulary a
    /// castle policy file names an override with.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "mechanical" => Some(Authority::Mechanical),
            "orchestrator" => Some(Authority::Orchestrator),
            "human" => Some(Authority::Human),
            _ => None,
        }
    }
}

/// The authority [`build`] assigns a violation `kind` in the absence of any
/// policy override — the single source of truth `crate::authority`'s
/// narrow-only validation checks a configured override against. Kept as a
/// standalone lookup (rather than requiring a live report) so policy can be
/// validated at daemon startup, before any violation has ever been observed.
/// `tests::builtin_authority_matches_every_kind_build_assigns` guards this
/// against drifting from what the per-kind constructors above actually set.
pub fn builtin_authority(kind: &str) -> Option<Authority> {
    match kind {
        kind::DELIVERED_BUT_OPEN => Some(Authority::Mechanical),
        kind::TERMINAL_ASSIGNEE_ACTIVE_WORK => Some(Authority::Orchestrator),
        kind::CONFLICT_HELD_LANDING => Some(Authority::Orchestrator),
        kind::TRACKER_CONTRADICTS_GIT => Some(Authority::Human),
        kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE => Some(Authority::Mechanical),
        _ => None,
    }
}

/// One detected contradiction between two or more of a repository's durable
/// views (or a durable view and git). Pure data — this module makes no state
/// changes and this type carries none.
#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    /// Stable identity (`<kind>:<subject>`), unchanged across repeated reads
    /// of unchanged state — the join key a caller uses to dedupe or diff two
    /// reports.
    pub id: String,
    /// The contradiction family; one of the [`kind`] constants.
    pub kind: String,
    /// Isolation scope (the repo name).
    pub scope: String,
    /// What the violation is about (a ticket id, or a branch name).
    pub subject: String,
    /// One-line human-readable explanation.
    pub detail: String,
    /// References to the records this violation was read from — tuple
    /// identities, agent names, commit shas — so a reader can go verify it
    /// independently rather than trust the summary.
    pub evidence: Vec<String>,
    pub authority: Authority,
}

/// The full report for one repository.
#[derive(Debug, Clone, Serialize)]
pub struct ConvergenceReport {
    pub scope: String,
    pub violations: Vec<Violation>,
}

/// Git's own answer to the questions this module needs asked of it,
/// pre-resolved by the caller (each answer costs a subprocess call) so
/// [`build`] stays pure over its inputs and unit-testable without a git
/// repository.
#[derive(Debug, Default, Clone)]
pub struct GitFacts {
    /// `(merge_commit, target) -> is merge_commit an ancestor of target?`
    /// Answers [`kind::TRACKER_CONTRADICTS_GIT`] for each ticket's delivery
    /// record. A pair absent from this map means the caller could not check
    /// it (unregistered or unopenable repo) — treated as "no evidence
    /// either way", never as a violation.
    pub is_ancestor: HashMap<(String, String), bool>,
    /// `(scope, branch)` pairs a dropped land has since actually reached its
    /// target by any route (merged, or the branch is gone) — the same
    /// self-clearing check `rk inbox`'s `unlanded-branch` row uses.
    pub cleared_branches: HashSet<(String, String)>,
}

fn short(sha: &str) -> &str {
    sha.get(0..8).unwrap_or(sha)
}

/// A ticket's own delivery record disagreeing with its own status field: the
/// record says the work landed (a non-empty merge commit), but the tracker
/// never closed it. In ordinary operation [`crate::tickets::Tickets::record_delivery`]
/// writes both fields atomically, so this reads as either a direct payload
/// edit bypassing that path, or a status regression after delivery — either
/// way it is safe to fix mechanically: the delivery record is the durable
/// proof, so the status field is what is wrong.
pub(crate) fn delivered_but_open(tickets: &[Tuple]) -> Vec<Violation> {
    tickets
        .iter()
        .filter_map(|t| {
            let record = crate::tickets::delivery_of(t)?;
            if record.merge_commit.is_empty() {
                return None;
            }
            let status = t
                .payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("open");
            if matches!(status, "done" | "closed") {
                return None;
            }
            Some(Violation {
                id: format!("{}:{}", kind::DELIVERED_BUT_OPEN, t.identity),
                kind: kind::DELIVERED_BUT_OPEN.into(),
                scope: t.scope.clone(),
                subject: t.identity.clone(),
                detail: format!(
                    "{} carries a delivery record (merge {} -> {}) but tracker status is still '{status}'",
                    t.identity,
                    short(&record.merge_commit),
                    record.target,
                ),
                evidence: vec![
                    format!("ticket:{}", t.identity),
                    format!("delivery.merge_commit:{}", record.merge_commit),
                    format!("delivery.target:{}", record.target),
                    format!("ticket.status:{status}"),
                ],
                authority: Authority::Mechanical,
            })
        })
        .collect()
}

/// The newest record for each agent name — preferring a live one over any
/// settled one, and the newest among settled ones. Mirrors
/// `Registry::get_any`'s resolution without requiring a live `Registry`, so
/// this stays a pure function over an owned slice.
fn latest_by(
    agents: &[AgentRecord],
    key: impl Fn(&AgentRecord) -> Option<&str>,
) -> HashMap<&str, &AgentRecord> {
    let mut map: HashMap<&str, &AgentRecord> = HashMap::new();
    for a in agents {
        let Some(k) = key(a) else { continue };
        map.entry(k)
            .and_modify(|existing: &mut &AgentRecord| {
                let existing_live = existing.state.is_live();
                let candidate_live = a.state.is_live();
                if !existing_live && (candidate_live || a.created_at > existing.created_at) {
                    *existing = a;
                }
            })
            .or_insert(a);
    }
    map
}

/// The ticket view says a ticket is actively being worked (`claimed` or
/// `in_progress`); the agent view says its owner already settled into a
/// terminal state (Completed/Failed/Stopped/Dismissed) and nothing has
/// picked the work back up. Mirrors `Server::ticket_reopen_sweep_at`'s
/// assignee resolution (assignee field, falling back to a live `task` match
/// only when assignee is absent) and its two hand-off carve-outs — a ticket
/// already recorded as landed, or still in flight through the landing queue,
/// is not abandoned, it has simply moved on from having a live owner.
///
/// Unlike the sweep, this raises immediately with no staleness timer: a
/// report is not a mutation, so there is no risk of reopening a ticket out
/// from under a rat that is mid-handoff — the sweep still owns that decision.
/// Resolve the agent that owns a ticket's active work — assignee field,
/// falling back to a live `task` match only when assignee is absent. Shared
/// by [`terminal_assignee_active_work`] (first read) and
/// `reconcile_repair::execute` (the fresh re-check immediately before a
/// stale-ownership CAS write), so both always resolve ownership the exact
/// same way.
pub(crate) fn resolve_owner<'a>(
    ticket_id: &str,
    assignee: Option<&str>,
    agents: &'a [AgentRecord],
) -> Option<&'a AgentRecord> {
    let by_name = latest_by(agents, |a| Some(a.name.as_str()));
    let by_task = latest_by(agents, |a| a.task.as_deref());
    match assignee {
        Some(name) => by_name.get(name).copied(),
        None => by_task.get(ticket_id).copied(),
    }
}

pub(crate) fn terminal_assignee_active_work(
    tickets: &[Tuple],
    agents: &[AgentRecord],
    landed_tickets: &HashSet<String>,
    queued_tickets: &HashSet<String>,
) -> Vec<Violation> {
    tickets
        .iter()
        .filter(|t| {
            matches!(
                t.payload.get("status").and_then(Value::as_str),
                Some("claimed") | Some("in_progress")
            )
        })
        .filter(|t| !landed_tickets.contains(&t.identity) && !queued_tickets.contains(&t.identity))
        .filter_map(|t| {
            let assignee = t.payload.get("assignee").and_then(Value::as_str);
            let agent = resolve_owner(&t.identity, assignee, agents)?;
            if !agent.state.is_archivable() {
                return None;
            }
            let mut evidence = vec![
                format!("ticket:{}", t.identity),
                format!("agent:{}", agent.name),
                format!("agent.state:{:?}", agent.state),
            ];
            if let Some(wi) = &agent.workflow_instance {
                evidence.push(format!("workflow_instance:{wi}"));
            }
            Some(Violation {
                id: format!("{}:{}", kind::TERMINAL_ASSIGNEE_ACTIVE_WORK, t.identity),
                kind: kind::TERMINAL_ASSIGNEE_ACTIVE_WORK.into(),
                scope: t.scope.clone(),
                subject: t.identity.clone(),
                detail: format!(
                    "{} is still '{}' but its owner {} settled to {:?} with no hand-off recorded",
                    t.identity,
                    t.payload
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("?"),
                    agent.name,
                    agent.state,
                ),
                evidence,
                authority: Authority::Orchestrator,
            })
        })
        .collect()
}

/// Re-derives `rk inbox`'s `unlanded-branch` row as an independent
/// convergence check: a `land` step reported a clean `{merged: false,
/// pr_opened: false}` — a conflict or refusal it deliberately does not treat
/// as an error, so a workflow can gate and retry — and the branch is still
/// standing outside its target with nothing else in the fleet tracking it
/// (TKT-171). `git.cleared_branches` (computed by the caller the same way
/// `rk inbox` does) makes this self-clearing: a later land or a hand-merge
/// retires the row without anything writing a "resolved" marker.
fn conflict_held_landing(lands: &[Tuple], git: &GitFacts) -> Vec<Violation> {
    crate::inbox::dropped_lands(lands)
        .into_iter()
        .filter_map(|t| {
            let branch = t.payload.get("branch").and_then(Value::as_str)?;
            if git
                .cleared_branches
                .contains(&(t.scope.clone(), branch.to_string()))
            {
                return None;
            }
            let target = t
                .payload
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("main");
            let why = t
                .payload
                .get("detail")
                .and_then(Value::as_str)
                .filter(|d| !d.trim().is_empty())
                .unwrap_or("no detail recorded");
            // Distinct held conflicts on the SAME branch (this one corrected,
            // then a later, genuinely new conflict) must never share an id:
            // `find_decision`'s terminal-replay lookup and the orchestrator
            // lease's cursor both key off `Violation::id` alone, so a shared
            // id would either replay the OLD chain's decision forever or
            // leave the new chain permanently behind the cursor. A land with
            // no `chain_key` (the pre-existing workflow-`land`-step source of
            // this same violation kind) falls back to the bare
            // `kind:scope:branch` id, unchanged from before this field
            // existed.
            //
            // The differentiator is the land tuple's OWN `t.id`, not the raw
            // `chain_key` string: `chain_key` is `ConflictContext::dispatch_key`,
            // which embeds a git `head_sha` — content that has no relationship
            // to time and can sort either side of an earlier chain's id.
            // `next_attention`'s cursor check is a bare lexicographic `>` over
            // the whole id, so an unlucky hash would make a genuinely later
            // conflict permanently unreachable past the previous chain's
            // cursor. `t.id` (`rk_core::id::RecordId`, a ULID) is guaranteed
            // unique AND lexicographically sortable by real creation time,
            // which is exactly what "distinct AND reachable" requires.
            let chain_key = t.payload.get("chain_key").and_then(Value::as_str);
            let id = match chain_key {
                Some(_) => format!(
                    "{}:{}:{}:{}",
                    kind::CONFLICT_HELD_LANDING,
                    t.scope,
                    branch,
                    t.id
                ),
                None => format!("{}:{}:{}", kind::CONFLICT_HELD_LANDING, t.scope, branch),
            };
            let mut evidence = vec![
                format!("branch_landed:{}", t.id),
                format!("branch:{branch}"),
                format!("target:{target}"),
            ];
            // Binds `Server::execute_orchestrator`/`orchestrator_attempt_hint`
            // to THIS exact chain: both only ever receive `(scope, subject)`
            // from the violation, and `subject` is just the branch name — the
            // same branch a genuinely later, distinct conflict can also carry.
            // Without the chain_key riding along in evidence, a decision
            // authorized against THIS violation could resolve its marker via
            // a fresh, independent "latest for branch" read at dispatch time
            // and act on whichever chain is newest then, not the one actually
            // named by `violation.id` — a TOCTOU window between the report
            // snapshot `attention.decide` authorized against and the
            // dispatch's own re-read moments later.
            if let Some(chain_key) = chain_key {
                evidence.push(format!("chain_key:{chain_key}"));
            }
            Some(Violation {
                id,
                kind: kind::CONFLICT_HELD_LANDING.into(),
                scope: t.scope.clone(),
                subject: branch.to_string(),
                detail: format!("land did not merge {branch} -> {target}: {why}"),
                evidence,
                authority: Authority::Orchestrator,
            })
        })
        .collect()
}

/// A ticket's delivery record claims a specific merge commit reached a
/// specific target; git's own ancestry check disagrees. This is the sharpest
/// contradiction the report can raise — either the record is wrong (a bad
/// write, a copy-paste) or the target's history no longer contains what it
/// once did (a rewritten branch, a force-push) — and both possibilities need
/// a human, not an automated fix, because "which one" changes what the right
/// repair is.
fn tracker_contradicts_git(tickets: &[Tuple], git: &GitFacts) -> Vec<Violation> {
    tickets
        .iter()
        .filter_map(|t| {
            let record = crate::tickets::delivery_of(t)?;
            if record.merge_commit.is_empty() {
                return None;
            }
            let key = (record.merge_commit.clone(), record.target.clone());
            // `None` means the caller could not check (unregistered or
            // unopenable repo) — absence of evidence, not evidence of a
            // contradiction, so no violation is raised.
            if git.is_ancestor.get(&key).copied().unwrap_or(true) {
                return None;
            }
            Some(Violation {
                id: format!("{}:{}", kind::TRACKER_CONTRADICTS_GIT, t.identity),
                kind: kind::TRACKER_CONTRADICTS_GIT.into(),
                scope: t.scope.clone(),
                subject: t.identity.clone(),
                detail: format!(
                    "{} claims delivery via {} -> {} but git does not show that commit as an ancestor of {}",
                    t.identity,
                    short(&record.merge_commit),
                    record.target,
                    record.target,
                ),
                evidence: vec![
                    format!("ticket:{}", t.identity),
                    format!("delivery.merge_commit:{}", record.merge_commit),
                    format!("delivery.target:{}", record.target),
                    "git.is_ancestor:false".to_string(),
                ],
                authority: Authority::Human,
            })
        })
        .collect()
}

/// The record for one *generation* of a name: the row whose
/// [`AgentRecord::spawn_id`] is the one the caller captured, not merely the
/// row that happens to hold the name now. Preferring a live row over a
/// settled one covers the archive/persist crash window, where the same
/// generation can briefly appear twice.
fn generation_of<'a>(
    agents: &'a [AgentRecord],
    name: &str,
    spawn: rk_core::id::SpawnId,
) -> Option<&'a AgentRecord> {
    agents
        .iter()
        .filter(|a| a.name == name && a.spawn_id() == spawn)
        .reduce(|best, a| {
            if !best.state.is_live() && a.state.is_live() {
                a
            } else {
                best
            }
        })
}

/// The agent generation a settled instance was actually supervising, or
/// `None` where the join cannot be made safely.
///
/// Names are reusable across generations — the registry frees a name once its
/// holder is archived (TKT-146) — so a name-only join lets a *newer namesake*
/// stand in for the generation the run really held. That is the exact shape
/// of a false [`kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE`], and this check's
/// authority is [`Authority::Mechanical`], so a false positive is a script
/// tearing down an innocent live rat. The join therefore keys on
/// `WorkflowContext::active_agent_spawn`/[`AgentRecord::spawn_id`], the same
/// durable generation identity `dismiss` guards itself with
/// (`Supervisor::dismiss_checked`). A recorded spawn id with no matching row
/// means that generation is no longer in the agent view at all — nothing to
/// report, never a fall back to the name.
///
/// Legacy instances snapshotted before `active_agent_spawn` existed carry no
/// generation identity, so they fall back to the name join this check has
/// always used, fenced by the instant the run settled: an agent record
/// created at or after that instant cannot be the one the run was
/// supervising, so a newer namesake is still never mistaken for a leak. An
/// instance with no settlement timestamp has no fence to apply and stays
/// silent rather than risk a mechanical dismissal on a name alone.
fn supervised_generation<'a>(
    i: &Instance,
    agents: &'a [AgentRecord],
    by_name: &HashMap<&str, &'a AgentRecord>,
) -> Option<&'a AgentRecord> {
    let active = i.context.active_agent.as_deref()?;
    match i.context.active_agent_spawn {
        Some(spawn) => generation_of(agents, active, spawn),
        None => {
            let settled_at = i.completed_at?;
            let candidate = by_name.get(active).copied()?;
            (candidate.created_at < settled_at).then_some(candidate)
        }
    }
}

/// A workflow instance's own ledger records the run as settled
/// (`Completed`/`Failed`), but the agent view shows its `active_agent` is
/// still live. Normal settlement dismisses the active agent as part of
/// finishing the run, so a live agent past that point means the engine
/// declared the run over while a worker under it is still running unwatched
/// — a supervision leak, not an in-progress hand-off. Archived instances are
/// excluded: they are historical record, not something to act on again.
///
/// The instance-to-agent join is by generation, not by name — see
/// [`supervised_generation`] for why a name alone cannot carry it.
fn workflow_settled_agent_still_live(
    instances: &[Instance],
    agents: &[AgentRecord],
) -> Vec<Violation> {
    let by_name = latest_by(agents, |a| Some(a.name.as_str()));
    instances
        .iter()
        .filter(|i| i.archived_at.is_none())
        .filter(|i| matches!(i.status, InstanceStatus::Completed | InstanceStatus::Failed))
        .filter_map(|i| {
            let agent = supervised_generation(i, agents, &by_name)?;
            if !agent.state.is_live() {
                return None;
            }
            Some(Violation {
                id: format!("{}:{}", kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE, i.id),
                kind: kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE.into(),
                scope: i.repo.clone(),
                subject: i.id.clone(),
                detail: format!(
                    "workflow instance {} ({}) settled to {:?} but its active agent {} is still {:?}",
                    i.id, i.workflow, i.status, agent.name, agent.state,
                ),
                evidence: vec![
                    format!("workflow_instance:{}", i.id),
                    format!("workflow_instance.status:{:?}", i.status),
                    format!("agent:{}", agent.name),
                    format!("agent.state:{:?}", agent.state),
                    // The generation the join was made on, so a reader can
                    // confirm this is the run's own agent and not a namesake.
                    format!("agent.spawn:{}", agent.spawn_id()),
                ],
                authority: Authority::Mechanical,
            })
        })
        .collect()
}

/// Aggregate every contradiction into one report. Pure over its inputs so it
/// can be unit-tested without a running daemon — the actual tuple scans and
/// git subprocess calls happen once, in the caller, before this runs.
///
/// `landed_tickets`/`queued_tickets` are the same hand-off carve-outs
/// `Server::ticket_reopen_sweep_at` computes: ticket ids the landing pipeline
/// has already recorded as landed, and ticket ids still in flight through the
/// landing queue, respectively. Both mean "this ticket's rat is gone because
/// it handed off", not abandonment.
///
/// `instances` is the durable workflow-instance ledger
/// (`WorkflowEngine::list_all`), the explicit workflow view this report
/// reconciles against the agent view.
#[allow(clippy::too_many_arguments)]
pub fn build(
    scope: &str,
    tickets: &[Tuple],
    agents: &[AgentRecord],
    lands: &[Tuple],
    landed_tickets: &HashSet<String>,
    queued_tickets: &HashSet<String>,
    instances: &[Instance],
    git: &GitFacts,
) -> ConvergenceReport {
    let mut violations = Vec::new();
    violations.extend(delivered_but_open(tickets));
    violations.extend(terminal_assignee_active_work(
        tickets,
        agents,
        landed_tickets,
        queued_tickets,
    ));
    violations.extend(conflict_held_landing(lands, git));
    violations.extend(tracker_contradicts_git(tickets, git));
    violations.extend(workflow_settled_agent_still_live(instances, agents));
    // Stable order: sorted by id, so two reads of unchanged state produce
    // byte-identical output regardless of scan order.
    violations.sort_by(|a, b| a.id.cmp(&b.id));
    ConvergenceReport {
        scope: scope.to_string(),
        violations,
    }
}

pub fn to_json(report: &ConvergenceReport) -> Value {
    serde_json::json!(report)
}

/// A concise human-readable rendering, one line per violation.
pub fn to_human(report: &ConvergenceReport) -> String {
    if report.violations.is_empty() {
        return format!("{}: converged — no contradictions detected\n", report.scope);
    }
    let mut out = format!(
        "{}: {} violation(s)\n",
        report.scope,
        report.violations.len()
    );
    for v in &report.violations {
        out.push_str(&format!(
            "[{:<12}] {:<32} {}\n",
            format!("{:?}", v.authority).to_lowercase(),
            v.kind,
            v.detail,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::AgentState;
    use chrono::Utc;
    use rk_core::tuple::{Category, Lifecycle};
    use std::path::PathBuf;

    fn ticket(id: &str, scope: &str, status: &str, extra: Value) -> Tuple {
        let mut payload = serde_json::json!({
            "title": "t",
            "status": status,
            "assignee": Value::Null,
        });
        if let Some(obj) = payload.as_object_mut() {
            if let Some(extra_obj) = extra.as_object() {
                for (k, v) in extra_obj {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        Tuple::new(Category::Task, scope, id, "castle", payload).with_lifecycle(Lifecycle::Session)
    }

    fn delivery_json(commit: &str, target: &str) -> Value {
        serde_json::json!({
            "delivery": {
                "merge_commit": commit,
                "branch": "rat/x/tkt-1",
                "target": target,
                "landed_at": "2026-08-19T00:00:00Z",
            }
        })
    }

    fn agent(name: &str, task: Option<&str>, state: AgentState) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            spawn: None,
            role: "worker".into(),
            coordination: None,
            harness: "claude".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/tmp/repo"),
            repo_name: "myrepo".into(),
            task: task.map(str::to_string),
            branch: None,
            fork_point: None,
            worktree: None,
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: Default::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
            liveness: Default::default(),
            transport_outage: None,
            recovery: None,
        }
    }

    fn instance(
        id: &str,
        repo: &str,
        status: InstanceStatus,
        active_agent: Option<&str>,
    ) -> Instance {
        Instance {
            id: id.into(),
            workflow: "some-workflow".into(),
            repo: repo.into(),
            coordinator: None,
            schedule: None,
            status,
            revision: 0,
            current_step: 1,
            total_steps: 1,
            context: crate::workflow_exec::WorkflowContext {
                active_agent: active_agent.map(str::to_string),
                ..Default::default()
            },
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "some-workflow".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: Default::default(),
            depth: 0,
            started_at: Utc::now(),
            completed_at: None,
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
        }
    }

    /// An instance carrying `agent`'s generation identity — the shape every
    /// instance written since `active_agent_spawn` landed has, and the shape
    /// the generation-safe join is built for. A terminal instance also gets
    /// the settlement timestamp the engine stamps on it.
    fn instance_for(id: &str, repo: &str, status: InstanceStatus, agent: &AgentRecord) -> Instance {
        let mut i = instance(id, repo, status, Some(&agent.name));
        i.context.active_agent_spawn = Some(agent.spawn_id());
        if matches!(status, InstanceStatus::Completed | InstanceStatus::Failed) {
            i.completed_at = Some(Utc::now());
        }
        i
    }

    /// A record with an explicitly minted generation. Two `agent()` calls in
    /// the same millisecond synthesise the *same* id from `created_at`, so
    /// any test about two generations of one name has to mint them.
    fn generation(name: &str, state: AgentState, created_at: chrono::DateTime<Utc>) -> AgentRecord {
        let mut a = agent(name, None, state);
        a.spawn = Some(rk_core::id::SpawnId::new());
        a.created_at = created_at;
        a
    }

    fn branch_landed(
        scope: &str,
        branch: &str,
        target: &str,
        merged: bool,
        pr_opened: bool,
    ) -> Tuple {
        Tuple::new(
            Category::Event,
            scope,
            "branch_landed",
            "castle",
            serde_json::json!({
                "branch": branch,
                "target": target,
                "merged": merged,
                "pr_opened": pr_opened,
                "detail": "conflict in crates/foo.rs",
            }),
        )
        .with_lifecycle(Lifecycle::Furniture)
    }

    #[test]
    fn delivered_but_open_flags_a_delivery_record_with_a_non_terminal_status() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main"),
        );
        let report = build(
            "myrepo",
            &[t],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].kind, kind::DELIVERED_BUT_OPEN);
        assert_eq!(report.violations[0].authority, Authority::Mechanical);
    }

    #[test]
    fn a_closed_delivered_ticket_is_not_a_violation() {
        let t = ticket("TKT-1", "myrepo", "closed", delivery_json("abc123", "main"));
        let report = build(
            "myrepo",
            &[t],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn an_undelivered_closed_ticket_is_not_this_violation() {
        // TKT-18/46/147's own class ("approved but never merged") is a
        // separate concern from this module's delivered-but-open check.
        let t = ticket("TKT-1", "myrepo", "closed", Value::Null);
        let report = build(
            "myrepo",
            &[t],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn terminal_assignee_still_owning_active_work_is_flagged() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            serde_json::json!({"assignee": "Whisker"}),
        );
        let a = agent("Whisker", Some("TKT-1"), AgentState::Dismissed);
        let report = build(
            "myrepo",
            &[t],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(
            report.violations[0].kind,
            kind::TERMINAL_ASSIGNEE_ACTIVE_WORK
        );
        assert_eq!(report.violations[0].authority, Authority::Orchestrator);
    }

    #[test]
    fn a_live_assignee_is_not_flagged() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            serde_json::json!({"assignee": "Whisker"}),
        );
        let a = agent("Whisker", Some("TKT-1"), AgentState::Running);
        let report = build(
            "myrepo",
            &[t],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn a_landed_ticket_is_not_flagged_even_with_a_terminal_owner() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            serde_json::json!({"assignee": "Whisker"}),
        );
        let a = agent("Whisker", Some("TKT-1"), AgentState::Dismissed);
        let mut landed = HashSet::new();
        landed.insert("TKT-1".to_string());
        let report = build(
            "myrepo",
            &[t],
            &[a],
            &[],
            &landed,
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty(), "hand-off, not abandonment");
    }

    #[test]
    fn assignee_absent_falls_back_to_a_task_match() {
        let t = ticket("TKT-1", "myrepo", "claimed", Value::Null);
        let a = agent("Whisker", Some("TKT-1"), AgentState::Failed);
        let report = build(
            "myrepo",
            &[t],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
    }

    #[test]
    fn a_dropped_land_uncleared_by_git_is_flagged() {
        let land = branch_landed("myrepo", "rat/x/tkt-1", "main", false, false);
        let report = build(
            "myrepo",
            &[],
            &[],
            &[land],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].kind, kind::CONFLICT_HELD_LANDING);
        assert_eq!(report.violations[0].subject, "rat/x/tkt-1");
        // Legacy path (no `chain_key`, the pre-existing workflow-`land`-step
        // source of this violation): the id stays the bare `kind:scope:branch`
        // shape it had before `chain_key` existed, not the tuple-id-suffixed
        // shape a conflict-correction hold now gets — an already-decided
        // legacy item must keep resolving to the same id it always has.
        assert_eq!(
            report.violations[0].id,
            format!("{}:myrepo:rat/x/tkt-1", kind::CONFLICT_HELD_LANDING)
        );
    }

    #[test]
    fn a_dropped_land_git_has_since_cleared_is_not_flagged() {
        let land = branch_landed("myrepo", "rat/x/tkt-1", "main", false, false);
        let mut cleared = HashSet::new();
        cleared.insert(("myrepo".to_string(), "rat/x/tkt-1".to_string()));
        let git = GitFacts {
            cleared_branches: cleared,
            ..Default::default()
        };
        let report = build(
            "myrepo",
            &[],
            &[],
            &[land],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &git,
        );
        assert!(report.violations.is_empty());
    }

    /// A second, genuinely later conflict on the same branch must get a
    /// violation id the orchestrator lease cursor can actually reach: the
    /// cursor comparison in `attention::next_attention` is a plain
    /// lexicographic `>` over the WHOLE id string
    /// (`kind:scope:branch:chain_key`), so if `chain_key` alone decided
    /// order, a later chain whose `head_sha` happens to sort lower than an
    /// earlier chain's would produce an id that is LESS than the cursor —
    /// permanently invisible to `attention.next`, exactly the bug this
    /// field was added to fix (see the comment on `chain_key` above). The id
    /// must instead be anchored to something that increases with real time
    /// regardless of what a `chain_key` string happens to contain — the
    /// land tuple's own `RecordId` (`rk_core::id::RecordId`, a ULID).
    /// `RecordId::floor_at` pins each tuple's id to an explicit,
    /// millisecond-distinct instant instead of `Tuple::new`'s current-time
    /// default: two back-to-back `Tuple::new` calls can legitimately land in
    /// the SAME millisecond, and a ULID's sub-millisecond ordering is random
    /// (`RecordId`'s own doc comment), which would make this test flaky if
    /// it depended on wall-clock scheduling for the property under test.
    #[test]
    fn a_later_conflict_chain_gets_an_id_that_sorts_after_an_earlier_terminal_chains_cursor() {
        use chrono::{TimeZone, Utc};
        let mut earlier = branch_landed("myrepo", "feature", "main", false, false);
        earlier.id = rk_core::id::RecordId::floor_at(Utc.timestamp_millis_opt(1_000).unwrap());
        // Deliberately sorts HIGH as a bare string, despite being the
        // earlier (lower-RecordId) tuple — the adversarial case a
        // content-derived chain_key cannot defend against.
        earlier.payload["chain_key"] = Value::String("zzzzzzzz-sha-from-first-conflict".into());
        let report_one = build(
            "myrepo",
            &[],
            &[],
            &[earlier.clone()],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        let cursor = report_one.violations[0].id.clone();

        let mut later = branch_landed("myrepo", "feature", "main", false, false);
        later.id = rk_core::id::RecordId::floor_at(Utc.timestamp_millis_opt(2_000).unwrap());
        // Deliberately sorts LOW as a bare string — a genuinely NEW, later
        // conflict (correcting the first) whose head_sha just happens to
        // hash lower.
        later.payload["chain_key"] = Value::String("aaaaaaaa-sha-from-second-conflict".into());
        assert!(
            later.id > earlier.id,
            "test setup: the second tuple must actually be minted later"
        );
        // Only the latest land per branch surfaces (`inbox::dropped_lands`);
        // the earlier chain's own violation has already disappeared from a
        // fresh report exactly as `next_attention`'s doc comment describes.
        let report_two = build(
            "myrepo",
            &[],
            &[],
            &[earlier, later],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert_eq!(report_two.violations.len(), 1);
        let next_id = report_two.violations[0].id.clone();

        assert!(
            next_id.as_str() > cursor.as_str(),
            "a later conflict chain's id ({next_id}) must sort after an earlier terminal \
             chain's cursor ({cursor}), or attention.next can never surface it again"
        );
    }

    #[test]
    fn a_merged_land_is_never_a_dropped_land() {
        let land = branch_landed("myrepo", "rat/x/tkt-1", "main", true, false);
        let report = build(
            "myrepo",
            &[],
            &[],
            &[land],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn git_disagreeing_with_a_delivery_record_is_flagged() {
        let t = ticket("TKT-1", "myrepo", "closed", delivery_json("abc123", "main"));
        let mut is_ancestor = HashMap::new();
        is_ancestor.insert(("abc123".to_string(), "main".to_string()), false);
        let git = GitFacts {
            is_ancestor,
            ..Default::default()
        };
        let report = build(
            "myrepo",
            &[t],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &git,
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].kind, kind::TRACKER_CONTRADICTS_GIT);
        assert_eq!(report.violations[0].authority, Authority::Human);
    }

    #[test]
    fn an_unchecked_delivery_record_is_not_flagged() {
        // No entry in `is_ancestor` for this pair means the caller could not
        // check (e.g. an unregistered repo) — absence of evidence must not
        // read as a contradiction.
        let t = ticket("TKT-1", "myrepo", "closed", delivery_json("abc123", "main"));
        let report = build(
            "myrepo",
            &[t],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn git_confirming_the_delivery_record_is_not_flagged() {
        let t = ticket("TKT-1", "myrepo", "closed", delivery_json("abc123", "main"));
        let mut is_ancestor = HashMap::new();
        is_ancestor.insert(("abc123".to_string(), "main".to_string()), true);
        let git = GitFacts {
            is_ancestor,
            ..Default::default()
        };
        let report = build(
            "myrepo",
            &[t],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &git,
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn repeated_reads_of_unchanged_state_are_identical() {
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main"),
        );
        let a = agent("Whisker", Some("TKT-1"), AgentState::Dismissed);
        let land = branch_landed("myrepo", "rat/y/tkt-2", "main", false, false);
        let build_it = || {
            build(
                "myrepo",
                std::slice::from_ref(&t),
                std::slice::from_ref(&a),
                std::slice::from_ref(&land),
                &HashSet::new(),
                &HashSet::new(),
                &[],
                &GitFacts::default(),
            )
        };
        let first = serde_json::to_string(&to_json(&build_it())).unwrap();
        let second = serde_json::to_string(&to_json(&build_it())).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn a_settled_instance_with_a_still_live_active_agent_is_flagged() {
        let a = agent("Whisker", None, AgentState::Running);
        let i = instance_for("wf-1", "myrepo", InstanceStatus::Completed, &a);
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(
            report.violations[0].kind,
            kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE
        );
        assert_eq!(report.violations[0].subject, "wf-1");
        assert_eq!(report.violations[0].authority, Authority::Mechanical);
    }

    #[test]
    fn a_failed_instance_with_a_still_live_active_agent_is_flagged() {
        let a = agent("Whisker", None, AgentState::Running);
        let i = instance_for("wf-1", "myrepo", InstanceStatus::Failed, &a);
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(
            report.violations[0].kind,
            kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE
        );
    }

    #[test]
    fn a_running_instance_with_a_live_active_agent_is_not_flagged() {
        let a = agent("Whisker", None, AgentState::Running);
        let i = instance_for("wf-1", "myrepo", InstanceStatus::Running, &a);
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn a_settled_instance_whose_agent_already_settled_too_is_not_flagged() {
        let a = agent("Whisker", None, AgentState::Dismissed);
        let i = instance_for("wf-1", "myrepo", InstanceStatus::Completed, &a);
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn a_settled_instance_with_no_active_agent_is_not_flagged() {
        let i = instance("wf-1", "myrepo", InstanceStatus::Completed, None);
        let report = build(
            "myrepo",
            &[],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn an_archived_settled_instance_with_a_live_agent_is_not_flagged() {
        // Historical record — nothing to act on again.
        let a = agent("Whisker", None, AgentState::Running);
        let mut i = instance_for("wf-1", "myrepo", InstanceStatus::Completed, &a);
        i.archived_at = Some(Utc::now());
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn an_old_settled_instance_does_not_flag_a_newer_live_namesake() {
        // The name "Whisker" outlived its first holder: the generation wf-old
        // supervised was dismissed, the registry freed the name, and a later
        // spawn took it. A name-only join reports the stranger — and this
        // violation's authority is Mechanical, so that report is a script
        // dismissing a live rat out from under whatever is actually
        // supervising it.
        let started = Utc::now();
        let supervised = generation("Whisker", AgentState::Dismissed, started);
        let i = instance_for("wf-old", "myrepo", InstanceStatus::Completed, &supervised);

        let namesake = generation(
            "Whisker",
            AgentState::Running,
            started + chrono::Duration::seconds(60),
        );
        assert_ne!(supervised.spawn_id(), namesake.spawn_id());

        let report = build(
            "myrepo",
            &[],
            &[supervised.clone(), namesake.clone()],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            std::slice::from_ref(&i),
            &GitFacts::default(),
        );
        assert!(
            report.violations.is_empty(),
            "a newer namesake is not the generation wf-old held: {:?}",
            report.violations,
        );

        // ...and the check is not merely silent: the same instance whose own
        // generation is still live is still a leak, namesake or not.
        let mut leaked = supervised;
        leaked.state = AgentState::Running;
        let report = build(
            "myrepo",
            &[],
            &[leaked, namesake],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(
            report.violations[0].kind,
            kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE
        );
        assert_eq!(report.violations[0].subject, "wf-old");
        assert_eq!(report.violations[0].authority, Authority::Mechanical);
    }

    #[test]
    fn a_settled_instance_whose_recorded_generation_is_absent_is_not_flagged() {
        // The generation wf-1 held is not in the agent view at all (archived
        // away). There is nothing to report, and the live namesake is not a
        // substitute for it.
        let gone = generation("Whisker", AgentState::Dismissed, Utc::now());
        let i = instance_for("wf-1", "myrepo", InstanceStatus::Completed, &gone);
        let namesake = generation("Whisker", AgentState::Running, Utc::now());
        let report = build(
            "myrepo",
            &[],
            &[namesake],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn a_legacy_instance_without_a_spawn_id_still_flags_its_own_live_agent() {
        // Pre-migration snapshot: no generation identity recorded, so the
        // name join stands — fenced by the settlement instant, which this
        // agent predates.
        let a = agent("Whisker", None, AgentState::Running);
        let mut i = instance("wf-1", "myrepo", InstanceStatus::Completed, Some("Whisker"));
        assert!(i.context.active_agent_spawn.is_none());
        i.completed_at = Some(a.created_at + chrono::Duration::seconds(60));
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(
            report.violations[0].kind,
            kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE
        );
    }

    #[test]
    fn a_legacy_instance_does_not_flag_a_namesake_spawned_after_it_settled() {
        // Same legacy shape, but the only record holding the name was created
        // after the run was already over, so it cannot be what the run held.
        let a = agent("Whisker", None, AgentState::Running);
        let mut i = instance("wf-1", "myrepo", InstanceStatus::Completed, Some("Whisker"));
        i.completed_at = Some(a.created_at - chrono::Duration::seconds(60));
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn a_legacy_instance_with_no_settlement_timestamp_is_not_flagged() {
        // No generation identity and no fence to apply: silence beats a
        // mechanical dismissal decided on a reusable name alone.
        let a = agent("Whisker", None, AgentState::Running);
        let i = instance("wf-1", "myrepo", InstanceStatus::Completed, Some("Whisker"));
        assert!(i.completed_at.is_none());
        let report = build(
            "myrepo",
            &[],
            &[a],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[i],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
    }

    #[test]
    fn builtin_authority_matches_every_kind_build_assigns() {
        // Every violation this module can produce must have a `builtin_authority`
        // entry equal to what its own constructor hardcodes, or a policy
        // override's narrow-only check would validate against a stale ceiling.
        let t = ticket(
            "TKT-1",
            "myrepo",
            "in_progress",
            delivery_json("abc123", "main"),
        );
        let agent_rec = agent("Whisker", Some("TKT-1"), AgentState::Dismissed);
        let land = branch_landed("myrepo", "rat/x/tkt-1", "main", false, false);
        let mut is_ancestor = HashMap::new();
        is_ancestor.insert(("def456".to_string(), "main".to_string()), false);
        let human_ticket = ticket("TKT-2", "myrepo", "closed", delivery_json("def456", "main"));
        let git = GitFacts {
            is_ancestor,
            ..Default::default()
        };
        let stale = agent("Gouda", None, AgentState::Running);
        let instance = instance_for("wf-1", "myrepo", InstanceStatus::Completed, &stale);
        let report = build(
            "myrepo",
            &[t, human_ticket],
            &[agent_rec, stale],
            &[land],
            &HashSet::new(),
            &HashSet::new(),
            &[instance],
            &git,
        );
        assert_eq!(report.violations.len(), 5, "{report:?}");
        for v in &report.violations {
            assert_eq!(
                builtin_authority(&v.kind),
                Some(v.authority),
                "builtin_authority drifted from build()'s own assignment for {}",
                v.kind
            );
        }
    }

    #[test]
    fn empty_state_converges_with_no_violations() {
        let report = build(
            "myrepo",
            &[],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
        assert!(to_human(&report).contains("converged"));
    }
}
