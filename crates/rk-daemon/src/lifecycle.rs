//! The canonical lifecycle-transition seam for delivery facts
//! (TKT-01M0P96ZSQAJGRE7WTGDBWAXJ9).
//!
//! Rat Kingdom's control plane had drifted into several independent writers
//! for what is logically one fact — "did this work land" — kept in sync only
//! by hand, at each call site, rather than by construction. This module is
//! the first seam factored out of that drift: the pure decision logic behind
//! [`crate::supervisor::Supervisor::finalize_delivery`], the single place a
//! landing (manual `rk land`/`rk land --force`, or the automatic
//! reactor-triggered pipeline) now derives the agent-side merge pointer from
//! the same commit it records as the ticket's durable
//! [`crate::tickets::DeliveryRecord`].
//!
//! ## Before / after ownership
//!
//! **Delivery** (a ticket's [`crate::tickets::DeliveryRecord`] and the
//! matching [`crate::agents::AgentRecord::merge_commit`] anchor `rk revert`
//! reads):
//!
//! - *Before*: two independent writers. `Tickets::record_delivery` (the
//!   canonical ticket-side write, unchanged by this module) was reachable
//!   from every landing path. `AgentRecord.merge_commit` was set only by
//!   `Supervisor::record_merge_for_branch`, itself reachable only from the
//!   two *manual* paths (`Supervisor::land`, `Supervisor::land_force`). The
//!   automatic/reactor-triggered path
//!   (`reactor::fire_land_action` -> `LandingPipeline::enqueue` ->
//!   `LandingPipeline::record_delivery`) wrote the ticket's delivery record
//!   but never touched the agent side at all — so `rk revert <agent>` on any
//!   agent landed through the pipeline (the common case; manual `rk land` is
//!   the exception) failed with "no recorded merge commit to revert" even
//!   though the ticket plainly showed delivered.
//! - *After*: one seam, `Supervisor::finalize_delivery`, called from both
//!   `LandingPipeline::record_delivery` and the two manual land paths. It
//!   writes the canonical ticket record (when a real ticket task is bound)
//!   and derives the agent's merge pointer from the identical commit in the
//!   same call, using [`resolve_merge_pointer`] to validate the resolved
//!   generation, its target, and its expected prior state before writing.
//!
//! **Ticket closure on `delivered-but-open`** (a ticket whose delivery
//! record already proves it shipped, but whose status field lags) is a
//! second, DOCUMENTED-BUT-NOT-YET-FIXED duplicate this same research found:
//! `Tickets::record_delivery`'s own close-on-deliver edge (ordinary
//! landing-time path), `reconcile_repair::plan`/`apply` (a real CAS with
//! git-ancestry/protected-path/replay-marker validation), and
//! `Server::execute_mechanical`'s `DELIVERED_BUT_OPEN` arm all independently
//! close the same ticket for the same condition — `execute_mechanical`
//! calls `Tickets::set_status("closed")` directly, no CAS, no evidence
//! check, no replay marker. A first attempt at deleting that pass-through
//! path (routing `execute_mechanical` through `reconcile_repair::plan`/
//! `apply` instead) was reverted before landing: it requires real git
//! evidence (`Server::merge_commit_ancestry`/`repair_git_facts`) that an
//! unregistered fixture repo — the shape
//! `authority_ladder.rs::mechanical_fixture_repairs_without_an_llm_and_replay_cannot_repeat_it`
//! exercises — cannot supply, which would flip that existing green test's
//! repair from `applied` to `held`. Fixing this needs either a fixture
//! change or a facts-optional variant of the plan, not a same-session
//! follow-on to the delivery fix above; left for a follow-up ticket rather
//! than risking an unverified regression here.
//!
//! ## Deliberately out of scope for this pass
//!
//! The sibling tickets under TKT-01M0FQ8BV7S558ZWZ99DBWA45E carve out the
//! rest of the lifecycle-writer map found while researching this ticket:
//! consolidating the landing-submission paths themselves
//! (TKT-01M0P96ZS78C4Y937F4QSWY9F4), absorbing the recovery/cleanup loops
//! (TKT-01M0P96ZZP6V57GFRHTX04DCPJ), and removing compatibility paths after a
//! persisted-state migration proof (TKT-01M0P97068ZS8KX28SEKHA64DV) — the
//! ticket that owns fully retiring `AgentRecord.merge_commit` as a stored
//! field rather than a value derived at write time. Agent-state transitions
//! (~31 call sites, already funneled through `Registry::update`) and
//! workflow settlement (already funneled through
//! `WorkflowEngine::try_update_with_reason`) are left alone: both already
//! have a single writer function today, and broadening this module to also
//! own *what* those ~39 call sites decide to write would be a rewrite, not
//! the surgical fix this ticket's concrete finding calls for.
//!
//! No CUE policy is consulted or duplicated here: the merge/delivery
//! evidence this module validates is already the *output* of landing's own
//! CUE-gated check resolution (`.rk/checks.cue`, loaded by
//! `rk_workflow::load_checks` and executed before a candidate ever reaches
//! `finalize_delivery`). This seam only decides how to record evidence that
//! already exists — it never resolves, executes, or replaces a named check.

use crate::agents::AgentRecord;
use std::path::Path;

/// What a delivery-finalization call should do to the agent-side merge
/// pointer for one landed `(repo_root, branch)`, decided purely from
/// already-known state — no I/O, no locks — so every branch is
/// unit-testable without a daemon and every call with the same inputs
/// reaches the same decision (replay-safe by construction, not by a
/// separately-maintained guard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MergePointerDecision {
    /// No agent generation targeting this `(repo_root, branch, target)`
    /// exists to derive a merge pointer onto — a bare named-branch land with
    /// no dispatched agent behind it. Nothing to write.
    NoTarget,
    /// The resolved generation's merge pointer already carries this exact
    /// commit — replaying the same delivery is a no-op, not a second write.
    AlreadyRecorded,
    /// The resolved generation has no merge pointer yet — the candidate
    /// commit becomes it.
    Set { agent: String },
    /// The resolved generation already carries a DIFFERENT merge commit.
    /// Two distinct deliveries are claiming the same generation, which means
    /// generation resolution itself is wrong somewhere upstream (branch name
    /// reused across two dispatches, a stale record). Fail closed: this is
    /// evidence of a bug, never a case to silently overwrite.
    Conflict { agent: String, recorded: String },
}

/// Resolve which agent generation (if any) a delivery for `(repo_root,
/// branch, target)` derives its merge pointer onto, and what to do about it.
///
/// `records` is scanned for the most recently created generation matching
/// all three of `repo_root`, `branch`, and `target` — the same "latest
/// generation wins" rule [`crate::supervisor::Supervisor::latest_task_record`]
/// already applies elsewhere, tightened with a `target` match the prior
/// ad hoc lookup (`record_merge_for_branch`) did not perform, so a branch
/// name reused across two differently-targeted dispatches cannot resolve to
/// the wrong generation. That resolved generation's existing `merge_commit`
/// is the expected prior state the candidate commit is validated against.
pub(crate) fn resolve_merge_pointer<'a>(
    records: impl Iterator<Item = &'a AgentRecord>,
    repo_root: &Path,
    branch: &str,
    target: &str,
    candidate_commit: &str,
) -> MergePointerDecision {
    let Some(record) = records
        .filter(|r| {
            r.repo_root == repo_root
                && r.branch.as_deref() == Some(branch)
                && r.target_branch == target
        })
        .max_by_key(|r| r.created_at)
    else {
        return MergePointerDecision::NoTarget;
    };
    match record.merge_commit.as_deref() {
        None => MergePointerDecision::Set {
            agent: record.name.clone(),
        },
        Some(existing) if existing == candidate_commit => MergePointerDecision::AlreadyRecorded,
        Some(existing) => MergePointerDecision::Conflict {
            agent: record.name.clone(),
            recorded: existing.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::AgentState;
    use chrono::{Duration, Utc};
    use rk_harness::TokenUsage;
    use std::path::PathBuf;

    fn record(name: &str, branch: &str, target: &str, age_secs: i64) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            spawn: None,
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/repo"),
            repo_name: "repo".into(),
            task: None,
            branch: Some(branch.to_string()),
            fork_point: None,
            worktree: None,
            target_branch: target.to_string(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: None,
            merge_commit: None,
            state: AgentState::Completed,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now() - Duration::seconds(age_secs),
            updated_at: Utc::now(),
            archived_at: None,
            liveness: Default::default(),
            transport_outage: None,
            recovery: None,
            recovery_receipt: None,
        }
    }

    #[test]
    fn no_matching_generation_is_no_target() {
        let records = vec![record("a1", "other-branch", "main", 0)];
        let decision = resolve_merge_pointer(
            records.iter(),
            Path::new("/repo"),
            "feature",
            "main",
            "sha1",
        );
        assert_eq!(decision, MergePointerDecision::NoTarget);
    }

    #[test]
    fn target_mismatch_does_not_resolve() {
        let records = vec![record("a1", "feature", "develop", 0)];
        let decision = resolve_merge_pointer(
            records.iter(),
            Path::new("/repo"),
            "feature",
            "main",
            "sha1",
        );
        assert_eq!(decision, MergePointerDecision::NoTarget);
    }

    #[test]
    fn unset_pointer_resolves_to_set() {
        let records = vec![record("a1", "feature", "main", 0)];
        let decision = resolve_merge_pointer(
            records.iter(),
            Path::new("/repo"),
            "feature",
            "main",
            "sha1",
        );
        assert_eq!(
            decision,
            MergePointerDecision::Set {
                agent: "a1".to_string()
            }
        );
    }

    #[test]
    fn latest_generation_wins_on_branch_reuse() {
        let older = record("a1", "feature", "main", 100);
        let newer = record("a2", "feature", "main", 0);
        let records = vec![older, newer];
        let decision = resolve_merge_pointer(
            records.iter(),
            Path::new("/repo"),
            "feature",
            "main",
            "sha1",
        );
        assert_eq!(
            decision,
            MergePointerDecision::Set {
                agent: "a2".to_string()
            }
        );
    }

    #[test]
    fn replaying_the_same_commit_is_idempotent() {
        let mut r = record("a1", "feature", "main", 0);
        r.merge_commit = Some("sha1".to_string());
        let records = vec![r];
        let decision = resolve_merge_pointer(
            records.iter(),
            Path::new("/repo"),
            "feature",
            "main",
            "sha1",
        );
        assert_eq!(decision, MergePointerDecision::AlreadyRecorded);

        // A second replay with the same input reaches the identical
        // decision — the whole point of a pure function over durable state.
        let decision2 = resolve_merge_pointer(
            records.iter(),
            Path::new("/repo"),
            "feature",
            "main",
            "sha1",
        );
        assert_eq!(decision2, MergePointerDecision::AlreadyRecorded);
    }

    #[test]
    fn conflicting_commit_fails_closed_instead_of_overwriting() {
        let mut r = record("a1", "feature", "main", 0);
        r.merge_commit = Some("sha-old".to_string());
        let records = vec![r];
        let decision = resolve_merge_pointer(
            records.iter(),
            Path::new("/repo"),
            "feature",
            "main",
            "sha-new",
        );
        assert_eq!(
            decision,
            MergePointerDecision::Conflict {
                agent: "a1".to_string(),
                recorded: "sha-old".to_string(),
            }
        );
    }
}
