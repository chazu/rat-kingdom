//! The attention ladder: pure logic over a `crate::reconcile::ConvergenceReport`
//! that turns its violations into resumable, policy-classified attention
//! items, and the durable decision-journal record shape every resolution
//! writes before it may be acknowledged.
//!
//! This module holds only what stays testable without a daemon — consuming
//! the next item off a cursor, and the pure mechanical repair a given
//! violation kind resolves to. The impure half (assembling the report,
//! writing the journal tuple, checking a lease, calling `Tickets`) lives in
//! `server.rs`, mirroring how `crate::reconcile` itself stays pure and
//! `server.rs::reconcile_value` does the impure assembly.

use crate::authority::AuthorityPolicy;
use crate::reconcile::{kind, Authority, ConvergenceReport, Violation};
use serde::Serialize;

/// One attention item ready for a caller to act on: a violation plus the
/// authority it is actually resolved under (after any policy override).
#[derive(Debug, Clone, Serialize)]
pub struct AttentionItem {
    #[serde(flatten)]
    pub violation: Violation,
    /// The effective authority, which may differ from `violation.authority`
    /// if `AuthorityPolicy` narrows it.
    pub effective_authority: Authority,
}

/// The next unresolved attention item after `cursor`, or `None` if the
/// report has none left. Violations are sorted by id
/// (`reconcile::build`'s own stable order), so "after `cursor`" is a
/// straightforward lexicographic comparison — this is the entire resumable
/// cursor: a caller that persists the last item's id and passes it back next
/// time picks up exactly where it left off, regardless of what else in the
/// report has changed in the meantime (a resolved item earlier in the sort
/// order simply disappears from the report; a cursor already past it is
/// unaffected).
pub fn next_attention(
    report: &ConvergenceReport,
    policy: &AuthorityPolicy,
    cursor: Option<&str>,
) -> Option<AttentionItem> {
    report
        .violations
        .iter()
        .find(|v| cursor.is_none_or(|c| v.id.as_str() > c))
        .map(|v| AttentionItem {
            violation: v.clone(),
            effective_authority: policy.effective_authority(v),
        })
}

/// The kinds of decisions the actual action-execution layer
/// (`server.rs::handle_attention_decide`) knows how to carry out. Kept as a
/// closed enum, not a free-form string, so "no repair registered for this
/// kind" is a compile-time-visible match arm rather than a string that
/// silently falls through — an unrecognized kind refuses rather than
/// guessing.
pub fn mechanical_action_for(v: &Violation) -> Option<&'static str> {
    match v.kind.as_str() {
        // The delivery record is the durable proof (`reconcile.rs`'s own
        // doc comment); the fix is exactly `Tickets::set_status(id, "closed")`.
        kind::DELIVERED_BUT_OPEN => Some("ticket.set_status(closed)"),
        _ => None,
    }
}

pub fn orchestrator_action_for(v: &Violation) -> Option<&'static str> {
    match v.kind.as_str() {
        // The mirror of `Tickets::claim`'s own CAS: hand the ticket back to
        // the backlog so a live rat can pick it back up. Exactly the
        // "redispatch" judgment call `reconcile.rs` names as this kind's
        // reason for Orchestrator authority.
        kind::TERMINAL_ASSIGNEE_ACTIVE_WORK => Some("ticket.reopen_if_in_progress"),
        // The dispatch decision `landing_conflict.rs`'s own doc comment
        // names as needing fleet-wide context: a bounded correction agent
        // was evidenced and held (`LandingPipeline::dispatch_held_conflict`)
        // but never spawned unattended — TKT-01M0E8PNFQZ70F3ZFG3KCS39ZG.
        kind::CONFLICT_HELD_LANDING => Some("conflict.dispatch_correction"),
        _ => None,
    }
}

/// Identity of the durable decision-journal event tuple
/// (`server.rs::handle_attention_decide`), one per resolved attention item,
/// keyed for idempotent replay by its `violation_id` payload field.
pub const DECISION_IDENTITY: &str = "orchestrator_decision";

/// The two dispositions a leased orchestrator may select via
/// `attention.decide`'s `disposition` parameter. `EXECUTE` (the default,
/// also what an omitted `disposition` means) is every existing
/// authority-ladder dispatch, unchanged. `DEFER_TO_HUMAN` is
/// TKT-01M0QD49GNDXM70ANERKZYXS3C's addition: a durable, non-mutating
/// hand-off to a human gate for an `Orchestrator`-authority item the caller
/// judges it cannot or should not execute — e.g. a legacy `conflict-held-landing`
/// item with no bounded chain marker, which `conflict.dispatch_correction`
/// would otherwise simply fail against forever, pinning the resumable
/// attention cursor behind an item nothing can ever execute.
pub const DISPOSITION_EXECUTE: &str = "execute";
pub const DISPOSITION_DEFER_TO_HUMAN: &str = "defer_to_human";

/// `action` recorded on a decision-journal entry written by the
/// `DEFER_TO_HUMAN` disposition.
pub const ACTION_DEFER_TO_HUMAN: &str = "attention.defer_to_human";

/// `decided_by` recorded on a decision journal entry for a mechanical
/// repair, which acts with no LLM/orchestrator session in the loop at all.
pub const DECIDED_BY_MECHANICAL: &str = "mechanical";

/// The message content a `Human`-authority attention item is refused with,
/// instead of ever calling into `Tickets`, the decision journal, or any
/// other mutation: what a human must decide, what is at stake if the wrong
/// call is made, and how to actually resolve it. Kept as a closed enum via
/// [`human_gate_for`]'s match, mirroring
/// [`mechanical_action_for`]/[`orchestrator_action_for`] — an unregistered
/// kind refuses outright rather than inventing generic gate text for it.
#[derive(Debug, Clone, Copy)]
pub struct HumanGate {
    /// What a human is actually being asked to decide.
    pub requested_decision: &'static str,
    /// What is affected if the wrong call is made here.
    pub blast_radius: &'static str,
    /// The concrete step a human takes to resolve this. Every violation
    /// `reconcile::build` raises is self-clearing (nothing writes a
    /// "resolved" marker for it) — once the human acts, the NEXT
    /// `reconcile.report` simply stops raising it.
    pub resolving_action: &'static str,
}

/// The human-gate template for a `Human`-authority violation kind, or `None`
/// if this build has no registered template for it — `server.rs`'s
/// `attention_decide` refuses outright in that case rather than writing a
/// gate record with invented text.
pub fn human_gate_for(v: &Violation) -> Option<HumanGate> {
    match v.kind.as_str() {
        kind::TRACKER_CONTRADICTS_GIT => Some(HumanGate {
            requested_decision: "confirm which record is correct: the ticket's delivery record (merge commit and target branch), or git's own ancestry history for that target",
            blast_radius: "the ticket's tracked delivery status, any mechanical repair that would trust it (delivered-but-open), and any reporting that depends on this ticket's convergence state",
            resolving_action: "correct the ticket's delivery record if it is wrong, or restore the claimed commit's ancestry on the target branch if history was rewritten, then re-run reconcile.report to confirm the contradiction has cleared",
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::{build, GitFacts};
    use rk_core::config::PolicyConfig;
    use std::collections::HashSet;

    fn ticket(id: &str, status: &str, extra: serde_json::Value) -> rk_core::tuple::Tuple {
        let mut payload = serde_json::json!({"title": "t", "status": status});
        if let (Some(obj), Some(extra)) = (payload.as_object_mut(), extra.as_object()) {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        }
        rk_core::tuple::Tuple::new(
            rk_core::tuple::Category::Task,
            "myrepo",
            id,
            "castle",
            payload,
        )
        .with_lifecycle(rk_core::tuple::Lifecycle::Session)
    }

    fn delivery(commit: &str) -> serde_json::Value {
        serde_json::json!({"delivery": {"merge_commit": commit, "branch": "b", "target": "main", "landed_at": "2026-08-19T00:00:00Z"}})
    }

    fn two_violation_report() -> ConvergenceReport {
        let a = ticket("TKT-1", "in_progress", delivery("abc"));
        let b = ticket("TKT-2", "in_progress", delivery("def"));
        build(
            "myrepo",
            &[a, b],
            &[],
            &[],
            &HashSet::new(),
            &HashSet::new(),
            &[],
            &GitFacts::default(),
        )
    }

    #[test]
    fn next_attention_starts_from_the_first_item_with_no_cursor() {
        let report = two_violation_report();
        let policy = AuthorityPolicy::default();
        let item = next_attention(&report, &policy, None).unwrap();
        assert_eq!(item.violation.id, report.violations[0].id);
    }

    #[test]
    fn next_attention_resumes_after_the_cursor() {
        let report = two_violation_report();
        let policy = AuthorityPolicy::default();
        let first = next_attention(&report, &policy, None).unwrap();
        let second = next_attention(&report, &policy, Some(&first.violation.id)).unwrap();
        assert_ne!(first.violation.id, second.violation.id);
        assert!(second.violation.id.as_str() > first.violation.id.as_str());
    }

    #[test]
    fn next_attention_is_none_once_the_cursor_passes_every_item() {
        let report = two_violation_report();
        let policy = AuthorityPolicy::default();
        let last_id = report.violations.last().unwrap().id.clone();
        assert!(next_attention(&report, &policy, Some(&last_id)).is_none());
    }

    #[test]
    fn effective_authority_reflects_a_policy_override() {
        let report = two_violation_report();
        let mut cfg = PolicyConfig::default();
        cfg.authority_overrides
            .insert(kind::DELIVERED_BUT_OPEN.into(), "human".into());
        let policy = AuthorityPolicy::from_config(&cfg).unwrap();
        let item = next_attention(&report, &policy, None).unwrap();
        assert_eq!(item.violation.authority, Authority::Mechanical);
        assert_eq!(item.effective_authority, Authority::Human);
    }

    #[test]
    fn mechanical_action_is_registered_for_delivered_but_open_only() {
        assert!(mechanical_action_for(&report_first_violation(kind::DELIVERED_BUT_OPEN)).is_some());
        assert!(orchestrator_action_for(&report_first_violation(
            kind::TERMINAL_ASSIGNEE_ACTIVE_WORK
        ))
        .is_some());
        assert!(
            orchestrator_action_for(&report_first_violation(kind::CONFLICT_HELD_LANDING)).is_some()
        );
        assert!(mechanical_action_for(&report_first_violation(
            kind::WORKFLOW_SETTLED_AGENT_STILL_LIVE
        ))
        .is_none());
    }

    #[test]
    fn human_gate_is_registered_for_tracker_contradicts_git_only() {
        assert!(human_gate_for(&report_first_violation(kind::TRACKER_CONTRADICTS_GIT)).is_some());
        assert!(human_gate_for(&report_first_violation(kind::DELIVERED_BUT_OPEN)).is_none());
        let gate = human_gate_for(&report_first_violation(kind::TRACKER_CONTRADICTS_GIT)).unwrap();
        assert!(!gate.requested_decision.is_empty());
        assert!(!gate.blast_radius.is_empty());
        assert!(!gate.resolving_action.is_empty());
    }

    fn report_first_violation(kind: &str) -> Violation {
        Violation {
            id: format!("{kind}:x"),
            kind: kind.to_string(),
            scope: "myrepo".into(),
            subject: "x".into(),
            detail: String::new(),
            evidence: Vec::new(),
            authority: crate::reconcile::builtin_authority(kind).unwrap(),
        }
    }
}
