//! The canonical lifecycle-transition seam for delivery facts
//! (TKT-01M0P96ZSQAJGRE7WTGDBWAXJ9): the pure decision logic behind
//! [`crate::supervisor::Supervisor::finalize_delivery`], the one place a
//! landing (manual `rk land`/`rk land --force`, or the automatic
//! reactor-triggered pipeline) derives the agent-side `merge_commit` from
//! the same commit recorded as the ticket's durable
//! [`crate::tickets::DeliveryRecord`]. Before this seam, only the two manual
//! land paths wrote the agent side at all, so `rk revert` on anything landed
//! automatically (the common case) failed with "no recorded merge commit."
//!
//! A second duplicate found by the same research — three independent
//! writers that can each close a `delivered-but-open` ticket
//! (`Tickets::record_delivery`, `reconcile_repair::plan`/`apply`, and
//! `Server::execute_mechanical`'s `DELIVERED_BUT_OPEN` arm) — is a documented
//! follow-up, not fixed here: routing `execute_mechanical` through
//! `reconcile_repair` needs git evidence an unregistered fixture repo can't
//! supply, which would regress `authority_ladder.rs`'s existing test.
//!
//! No CUE policy is consulted here: this seam only records evidence that
//! `.rk/checks.cue`-gated landing already produced.

use crate::agents::AgentRecord;
use rk_core::id::SpawnId;
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

/// How a `finalize_delivery` conflict (a resolved generation already
/// carries a DIFFERENT merge commit than the candidate) should be treated
/// — TKT-jonis-faror-zufuj. A resumed generation that legitimately delivers
/// a second, later commit to the same branch/target looks identical, at the
/// registry level, to two unrelated deliveries racing onto the same
/// generation: both are "the recorded commit differs from the candidate."
/// Only a delivery whose queue entry already passed this repository's
/// native gate/review carries evidence strong enough to tell them apart via
/// git ancestry; the operator's ungated `land_force` escape hatch does not,
/// so it must keep failing closed onto explicit manual reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SuccessorPolicy {
    /// Any differing commit fails closed — used by `land_force`.
    FailClosed,
    /// A differing commit that is a proven git descendant of what's
    /// recorded is accepted as this generation's next delivery; a
    /// differing commit that what's recorded already descends from is a
    /// stale/late receipt replay and is silently dropped. Used by the
    /// native landing pipeline.
    AdvanceOnDescendant,
}

/// What a `MergePointerDecision::Conflict` resolves to once the two
/// commits' git ancestry is known. Pure and unit-testable: the caller
/// supplies the two `is_ancestor` facts already computed against the repo,
/// so this function itself does no I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SuccessorClassification {
    /// The candidate is a proven git descendant of what's recorded: a
    /// genuine successor delivery from the same, resumed generation.
    Advance,
    /// What's recorded is a proven git descendant of the candidate: the
    /// candidate is an older, already-superseded receipt replaying late.
    /// Never rolls a newer projection backward.
    StaleReplay,
    /// Neither commit is an ancestor of the other (or the policy forbids
    /// classifying at all): unrelated histories claiming the same
    /// generation. Fail closed exactly as the original Conflict behavior.
    Unrelated,
}

/// Classify a `Conflict` using already-computed git ancestry facts.
/// `recorded_is_ancestor_of_candidate` / `candidate_is_ancestor_of_recorded`
/// come from `Repo::is_ancestor`, which treats equal revisions as mutually
/// ancestral — but `resolve_merge_pointer` already routes an equal
/// candidate to `AlreadyRecorded` before a `Conflict` is ever produced, so
/// both true together does not arise here in practice.
pub(crate) fn classify_successor(
    policy: SuccessorPolicy,
    recorded_is_ancestor_of_candidate: bool,
    candidate_is_ancestor_of_recorded: bool,
) -> SuccessorClassification {
    match policy {
        SuccessorPolicy::FailClosed => SuccessorClassification::Unrelated,
        SuccessorPolicy::AdvanceOnDescendant => {
            match (
                recorded_is_ancestor_of_candidate,
                candidate_is_ancestor_of_recorded,
            ) {
                (true, false) => SuccessorClassification::Advance,
                (false, true) => SuccessorClassification::StaleReplay,
                _ => SuccessorClassification::Unrelated,
            }
        }
    }
}

/// Resolve which agent generation (if any) a delivery for `(repo_root,
/// branch, target)` derives its merge pointer onto, and what to do about it.
///
/// `exact_spawn`, when present, is the source generation's own
/// [`SpawnId`] straight off the triggering `harness_result` — resolution
/// matches that exact generation, never a guess by recency, so a branch name
/// reused across two dispatches cannot resolve onto the wrong one.
///
/// `exact_spawn: None` means the delivery is not attributable to an agent
/// generation (for example, an explicitly ticket-bound recovery branch).
/// Branch/name/recency guesses are deliberately forbidden: ticket delivery is
/// still recorded, but no agent merge pointer is derived without exact proof.
pub(crate) fn resolve_merge_pointer<'a>(
    records: impl Iterator<Item = &'a AgentRecord>,
    repo_root: &Path,
    branch: &str,
    target: &str,
    candidate_commit: &str,
    exact_spawn: Option<SpawnId>,
) -> MergePointerDecision {
    let Some(exact_spawn) = exact_spawn else {
        return MergePointerDecision::NoTarget;
    };
    let found = records
        .filter(|r| {
            r.repo_root == repo_root
                && r.branch.as_deref() == Some(branch)
                && r.target_branch == target
        })
        .find(|r| r.spawn_id() == exact_spawn);
    let Some(record) = found else {
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
            spawn: Some(rk_core::id::SpawnId::new()),
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
            current_attempt: None,
        }
    }

    /// One row per distinct invariant `resolve_merge_pointer` must uphold —
    /// see each row's comment for which. All resolve against `("feature",
    /// "main")`; only the candidate records, exact-spawn hint, and candidate
    /// commit vary.
    #[test]
    fn resolve_merge_pointer_decisions() {
        let mut exact_gen = record("a1", "feature", "main", 100);
        exact_gen.spawn = Some(SpawnId::new());
        let exact = exact_gen.spawn.unwrap();
        let mut recorded = record("a1", "feature", "main", 0);
        recorded.merge_commit = Some("sha-old".into());

        type Case<'a> = (
            &'a str,
            Vec<AgentRecord>,
            Option<SpawnId>,
            &'a str,
            MergePointerDecision,
        );
        let cases: Vec<Case> = vec![
            (
                "branch mismatch has no target",
                vec![record("a1", "other-branch", "main", 0)],
                None,
                "sha1",
                MergePointerDecision::NoTarget,
            ),
            (
                "target mismatch has no target",
                vec![record("a1", "feature", "develop", 0)],
                None,
                "sha1",
                MergePointerDecision::NoTarget,
            ),
            (
                "missing exact generation derives no pointer",
                vec![record("a1", "feature", "main", 0)],
                None,
                "sha1",
                MergePointerDecision::NoTarget,
            ),
            (
                "branch reuse without exact generation derives no pointer",
                vec![
                    record("a1", "feature", "main", 100),
                    record("a2", "feature", "main", 0),
                ],
                None,
                "sha1",
                MergePointerDecision::NoTarget,
            ),
            (
                "an exact spawn resolves that generation even when older",
                vec![exact_gen.clone(), record("a2", "feature", "main", 0)],
                Some(exact),
                "sha1",
                MergePointerDecision::Set { agent: "a1".into() },
            ),
            (
                "an exact spawn with no matching record has no target",
                vec![record("a1", "feature", "main", 0)],
                Some(SpawnId::new()),
                "sha1",
                MergePointerDecision::NoTarget,
            ),
            (
                "replaying the recorded commit is idempotent",
                vec![recorded.clone()],
                Some(recorded.spawn_id()),
                "sha-old",
                MergePointerDecision::AlreadyRecorded,
            ),
            (
                "a different candidate against a recorded commit fails closed",
                vec![recorded.clone()],
                Some(recorded.spawn_id()),
                "sha-new",
                MergePointerDecision::Conflict {
                    agent: "a1".into(),
                    recorded: "sha-old".into(),
                },
            ),
        ];

        for (name, records, exact_spawn, candidate, expected) in cases {
            let decision = resolve_merge_pointer(
                records.iter(),
                Path::new("/repo"),
                "feature",
                "main",
                candidate,
                exact_spawn,
            );
            assert_eq!(decision, expected, "case: {name}");
        }
    }

    /// One row per distinct invariant `classify_successor` must uphold
    /// (TKT-jonis-faror-zufuj): `FailClosed` never advances or drops
    /// anything regardless of ancestry, and `AdvanceOnDescendant` tells a
    /// genuine resumed-generation successor apart from a stale replay and
    /// from a truly unrelated commit using only the two ancestry facts.
    #[test]
    fn classify_successor_decisions() {
        let cases: [(&str, SuccessorPolicy, bool, bool, SuccessorClassification); 6] = [
            (
                "fail-closed never advances even on a proven descendant",
                SuccessorPolicy::FailClosed,
                true,
                false,
                SuccessorClassification::Unrelated,
            ),
            (
                "fail-closed never treats an ancestor candidate as stale either",
                SuccessorPolicy::FailClosed,
                false,
                true,
                SuccessorClassification::Unrelated,
            ),
            (
                "a candidate descending from the recorded commit advances",
                SuccessorPolicy::AdvanceOnDescendant,
                true,
                false,
                SuccessorClassification::Advance,
            ),
            (
                "a candidate the recorded commit already descends from is a stale replay",
                SuccessorPolicy::AdvanceOnDescendant,
                false,
                true,
                SuccessorClassification::StaleReplay,
            ),
            (
                "neither ancestor of the other is unrelated",
                SuccessorPolicy::AdvanceOnDescendant,
                false,
                false,
                SuccessorClassification::Unrelated,
            ),
            (
                "both ancestors of each other (unreachable in practice) fails closed, not advances",
                SuccessorPolicy::AdvanceOnDescendant,
                true,
                true,
                SuccessorClassification::Unrelated,
            ),
        ];

        for (name, policy, recorded_is_ancestor, candidate_is_ancestor, expected) in cases {
            let got = classify_successor(policy, recorded_is_ancestor, candidate_is_ancestor);
            assert_eq!(got, expected, "case: {name}");
        }
    }
}
