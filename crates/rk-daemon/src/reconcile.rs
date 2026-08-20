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
//! Four contradiction families, chosen because each is a gap between two
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
//!
//! Every violation names a stable identity (`kind:subject`, unchanged across
//! repeated reads of unchanged state), the evidence it was read from, and a
//! suggested [`Authority`] — who can safely act on it without a human in the
//! loop.

use crate::agents::AgentRecord;
use rk_core::tuple::Tuple;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub mod kind {
    pub const DELIVERED_BUT_OPEN: &str = "delivered-but-open";
    pub const TERMINAL_ASSIGNEE_ACTIVE_WORK: &str = "terminal-assignee-active-work";
    pub const CONFLICT_HELD_LANDING: &str = "conflict-held-landing";
    pub const TRACKER_CONTRADICTS_GIT: &str = "tracker-contradicts-git-history";
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
fn delivered_but_open(tickets: &[Tuple]) -> Vec<Violation> {
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
fn latest_by<'a>(
    agents: &'a [AgentRecord],
    key: impl Fn(&AgentRecord) -> Option<&str>,
) -> HashMap<&'a str, &'a AgentRecord> {
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
fn terminal_assignee_active_work(
    tickets: &[Tuple],
    agents: &[AgentRecord],
    landed_tickets: &HashSet<String>,
    queued_tickets: &HashSet<String>,
) -> Vec<Violation> {
    let by_name = latest_by(agents, |a| Some(a.name.as_str()));
    let by_task = latest_by(agents, |a| a.task.as_deref());

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
            let agent = match assignee {
                Some(name) => by_name.get(name).copied(),
                None => by_task.get(t.identity.as_str()).copied(),
            }?;
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
            Some(Violation {
                id: format!("{}:{}:{}", kind::CONFLICT_HELD_LANDING, t.scope, branch),
                kind: kind::CONFLICT_HELD_LANDING.into(),
                scope: t.scope.clone(),
                subject: branch.to_string(),
                detail: format!("land did not merge {branch} -> {target}: {why}"),
                evidence: vec![
                    format!("branch_landed:{}", t.id),
                    format!("branch:{branch}"),
                    format!("target:{target}"),
                ],
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

/// Aggregate every contradiction into one report. Pure over its inputs so it
/// can be unit-tested without a running daemon — the actual tuple scans and
/// git subprocess calls happen once, in the caller, before this runs.
///
/// `landed_tickets`/`queued_tickets` are the same hand-off carve-outs
/// `Server::ticket_reopen_sweep_at` computes: ticket ids the landing pipeline
/// has already recorded as landed, and ticket ids still in flight through the
/// landing queue, respectively. Both mean "this ticket's rat is gone because
/// it handed off", not abandonment.
#[allow(clippy::too_many_arguments)]
pub fn build(
    scope: &str,
    tickets: &[Tuple],
    agents: &[AgentRecord],
    lands: &[Tuple],
    landed_tickets: &HashSet<String>,
    queued_tickets: &HashSet<String>,
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
        }
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
            &GitFacts::default(),
        );
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].kind, kind::CONFLICT_HELD_LANDING);
        assert_eq!(report.violations[0].subject, "rat/x/tkt-1");
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
            &git,
        );
        assert!(report.violations.is_empty());
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
                &GitFacts::default(),
            )
        };
        let first = serde_json::to_string(&to_json(&build_it())).unwrap();
        let second = serde_json::to_string(&to_json(&build_it())).unwrap();
        assert_eq!(first, second);
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
            &GitFacts::default(),
        );
        assert!(report.violations.is_empty());
        assert!(to_human(&report).contains("converged"));
    }
}
