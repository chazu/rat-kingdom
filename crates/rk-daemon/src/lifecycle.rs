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

/// Resolve which agent generation (if any) a delivery for `(repo_root,
/// branch, target)` derives its merge pointer onto, and what to do about it.
///
/// `exact_spawn`, when present, is the source generation's own
/// [`SpawnId`] straight off the triggering `harness_result` — resolution
/// matches that exact generation, never a guess by recency, so a branch name
/// reused across two dispatches cannot resolve onto the wrong one.
///
/// `exact_spawn: None` is the explicit compatibility fallback for callers
/// with no exact generation to resolve onto: a queue entry written before
/// source-generation capture existed, or a manual `rk land`/`rk land
/// --force` invocation, which only ever names a branch. It falls back to the
/// same "newest matching generation" rule
/// [`crate::supervisor::Supervisor::latest_task_record`] applies elsewhere.
/// Callers observe when this path is taken (see `record_delivery`'s log) and
/// it is removable once no caller ever passes `None` for an automatic
/// candidate.
pub(crate) fn resolve_merge_pointer<'a>(
    records: impl Iterator<Item = &'a AgentRecord>,
    repo_root: &Path,
    branch: &str,
    target: &str,
    candidate_commit: &str,
    exact_spawn: Option<SpawnId>,
) -> MergePointerDecision {
    let mut matching = records.filter(|r| {
        r.repo_root == repo_root && r.branch.as_deref() == Some(branch) && r.target_branch == target
    });
    let found = match exact_spawn {
        Some(spawn) => matching.find(|r| r.spawn_id() == spawn),
        None => matching.max_by_key(|r| r.created_at),
    };
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
                "unset pointer resolves to set",
                vec![record("a1", "feature", "main", 0)],
                None,
                "sha1",
                MergePointerDecision::Set { agent: "a1".into() },
            ),
            (
                "no exact spawn falls back to the newest generation on branch reuse",
                vec![
                    record("a1", "feature", "main", 100),
                    record("a2", "feature", "main", 0),
                ],
                None,
                "sha1",
                MergePointerDecision::Set { agent: "a2".into() },
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
                None,
                "sha-old",
                MergePointerDecision::AlreadyRecorded,
            ),
            (
                "a different candidate against a recorded commit fails closed",
                vec![recorded.clone()],
                None,
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
}
