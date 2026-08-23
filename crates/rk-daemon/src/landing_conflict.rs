//! Pure policy and evidence formatting for unattended merge-CONFLICT
//! recovery — the landing pipeline's other bounded-correction tracer,
//! alongside [`crate::landing_rework`] (which corrects a reviewer's REWORK
//! verdict). A conflict never even builds a candidate, so there is no review
//! artifact to read: authority here is judged from the conflict's own
//! evidence — whether it touches a protected path, whether its blast radius
//! is within the repo's declared budget, and whether git's own conflict
//! report is even readable — never from reviewer notes.
//!
//! Deliberately reuses [`crate::landing_rework::ReworkRoute`] and
//! [`crate::landing_rework::Withheld`] rather than re-declaring structurally
//! identical types: "dispatch this bounded attempt" and "hold for a human,
//! for this reason" are the same two outcomes whether the correction is
//! answering a reviewer or a merge conflict. Durable markers, spawning, and
//! corrected-branch resubmission remain in [`crate::landing`].

use crate::landing_rework::{ReworkRoute, Withheld};
use serde_json::{json, Value};

/// Durable, repo-scoped routing marker written before a conflict-correction
/// spawn. Distinct from [`crate::landing_rework::REWORK_DISPATCH_IDENTITY`]:
/// a conflict and a review verdict are different chains with independent
/// budgets, even when they land on the same branch.
pub(crate) const CONFLICT_DISPATCH_IDENTITY: &str = "landing_conflict_rework_dispatch";

/// Marker state written when authority resolves to `Orchestrator` but
/// nothing has dispatched yet — evidence collection and ticket filing are
/// daemon-owned and complete, but the actual correction spawn is withheld
/// until a leased, fenced orchestrator decision authorizes it through
/// `attention.decide`. Distinct from `"dispatching"`/`"dispatched"`, which
/// are only ever written by that authorized dispatch itself
/// (`LandingPipeline::dispatch_held_conflict`), never by `route_conflict`.
pub(crate) const CONFLICT_STATE_AWAITING_DECISION: &str = "awaiting-orchestrator-decision";

/// How many characters of git's own conflict report are kept in the
/// structured recovery item and the agent prompt. `rk-git`'s own
/// `failure_reason` already caps to a few lines, but this is a second,
/// independent bound — evidence handed to an unattended orchestrator must
/// stay bounded even if the upstream cap is ever loosened.
const CONFLICT_DETAIL_MAX_CHARS: usize = 2000;

/// Cap `detail` to [`CONFLICT_DETAIL_MAX_CHARS`], marking truncation
/// visibly rather than silently dropping the tail.
pub(crate) fn bound_conflict_detail(detail: &str) -> String {
    let trimmed = detail.trim();
    if trimmed.chars().count() <= CONFLICT_DETAIL_MAX_CHARS {
        return trimmed.to_string();
    }
    let head: String = trimmed.chars().take(CONFLICT_DETAIL_MAX_CHARS).collect();
    format!("{head}… (truncated)")
}

/// Stable ticket dedupe key. Target and task distinguish separate landing
/// chains; distinct namespace from [`crate::landing_rework::ticket_coalesce_key`]
/// so a conflict and a review-rework on the same branch/head never coalesce
/// onto the same follow-up ticket.
pub(crate) fn ticket_coalesce_key(
    repo: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    task: &str,
) -> String {
    format!("landing-conflict-rework:{repo}:{branch}:{head_sha}:{target}:{task}")
}

/// Activated repository bounds for unattended conflict recovery.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ConflictPolicy {
    /// `false` files the ticket and holds the branch without dispatching.
    pub(crate) auto_dispatch: bool,
    /// Hard ceiling across a conflict→correction→re-land chain.
    pub(crate) max_attempts: u32,
    /// Whole-dollar chain ceiling; `0` means unlimited and preserves policy `Eq`.
    pub(crate) max_usd: u32,
}

impl ConflictPolicy {
    pub(crate) fn from_landing(policy: &rk_workflow::LandingPolicy) -> Self {
        Self {
            auto_dispatch: policy.conflict_rework_auto_dispatch,
            max_attempts: policy.max_conflict_rework_attempts,
            max_usd: policy.conflict_rework_max_usd,
        }
    }
}

impl Default for ConflictPolicy {
    fn default() -> Self {
        Self::from_landing(&rk_workflow::LandingPolicy::default())
    }
}

/// Evidence about one conflicted merge attempt, gathered by the caller
/// (which has git and policy access) and judged here, pure.
#[derive(Debug, Clone)]
pub(crate) struct ConflictEvidence<'a> {
    /// Bounded (see [`bound_conflict_detail`]) conflict report from git.
    pub(crate) conflict_detail: &'a str,
    /// Whether any file changed between the branch and its target matches
    /// the repo's `protectedPaths` ERE.
    pub(crate) protected_path_hit: bool,
    pub(crate) diff_files: u64,
    pub(crate) diff_lines: u64,
    /// `0` disables the corresponding budget, matching `LandingPolicy`.
    pub(crate) max_diff_files: u64,
    pub(crate) max_diff_lines: u64,
}

fn human(
    code: &'static str,
    detail: impl Into<String>,
    decision: impl Into<String>,
) -> (crate::reconcile::Authority, Option<Withheld>) {
    (
        crate::reconcile::Authority::Human,
        Some(Withheld {
            code,
            detail: detail.into(),
            decision: decision.into(),
        }),
    )
}

fn withhold(
    code: &'static str,
    detail: impl Into<String>,
    decision: impl Into<String>,
) -> ReworkRoute {
    ReworkRoute::Withhold(Withheld {
        code,
        detail: detail.into(),
        decision: decision.into(),
    })
}

/// Classify a conflict on the authority ladder, failing closed. A protected
/// path, an over-budget diff, or unreadable conflict evidence is never
/// bounded enough for unattended resolution, no matter what policy says.
pub(crate) fn classify_conflict_authority(
    evidence: &ConflictEvidence,
) -> (crate::reconcile::Authority, Option<Withheld>) {
    if evidence.conflict_detail.trim().is_empty() {
        return human(
            "unbounded-conflict-evidence",
            "git reported no readable conflict detail, so there is nothing bounded to hand a \
             correction agent",
            "read the branch yourself and either dispatch a correction with explicit \
             instructions, or resolve/abandon the branch by hand",
        );
    }
    if evidence.protected_path_hit {
        return human(
            "protected-path-impact",
            "the conflict touches a file matched by this repository's protectedPaths policy",
            "review the protected-path impact yourself, then dispatch a correction or resolve \
             the branch by hand",
        );
    }
    let files_over = evidence.max_diff_files > 0 && evidence.diff_files > evidence.max_diff_files;
    let lines_over = evidence.max_diff_lines > 0 && evidence.diff_lines > evidence.max_diff_lines;
    if files_over || lines_over {
        return human(
            "blast-radius-uncertain",
            format!(
                "the conflicted diff is {} file(s) / {} line(s), over this repository's \
                 maxDiffFiles/maxDiffLines budget of {}/{}",
                evidence.diff_files,
                evidence.diff_lines,
                evidence.max_diff_files,
                evidence.max_diff_lines
            ),
            "judge the blast radius yourself, then dispatch a correction or resolve the branch \
             by hand",
        );
    }
    (crate::reconcile::Authority::Orchestrator, None)
}

/// Route one conflict. Authority precedes durable attempt and chain-spend caps.
pub(crate) fn route(
    policy: &ConflictPolicy,
    evidence: &ConflictEvidence,
    attempts_used: u32,
    spent_usd: f64,
) -> ReworkRoute {
    if !policy.auto_dispatch {
        return withhold(
            "auto-dispatch-disabled",
            "this repository's landing policy has conflictReworkAutoDispatch disabled",
            "dispatch the correction ticket by hand, or enable conflictReworkAutoDispatch in \
             .rk/repo.cue",
        );
    }
    if let (crate::reconcile::Authority::Human, Some(withheld)) =
        classify_conflict_authority(evidence)
    {
        return ReworkRoute::Withhold(withheld);
    }
    if attempts_used >= policy.max_attempts {
        return withhold(
            "attempts-exhausted",
            format!(
                "this branch has already had {attempts_used} automatic conflict-correction \
                 attempt(s), at or over the repository cap of {}",
                policy.max_attempts
            ),
            "decide whether this branch is converging: dispatch another correction by hand, \
             resolve it yourself, or abandon the branch",
        );
    }
    if policy.max_usd > 0 && spent_usd >= f64::from(policy.max_usd) {
        return withhold(
            "budget-exhausted",
            format!(
                "this branch's conflict/correction chain has already spent ${spent_usd:.2}, at \
                 or over the repository cap of ${}",
                policy.max_usd
            ),
            "decide whether this branch is worth more spend: dispatch another correction by \
             hand, resolve it yourself, or abandon the branch",
        );
    }
    ReworkRoute::Dispatch {
        attempt: attempts_used + 1,
    }
}

/// Exact conflict identity and evidence used by dispatch and escalation —
/// also the structured recovery item: repository, source, target, fork
/// point, exact heads, and bounded conflict evidence all live here and flow
/// straight into [`ConflictContext::marker_payload`].
#[derive(Debug, Clone)]
pub(crate) struct ConflictContext {
    pub(crate) repo: String,
    /// Filesystem root `Repo::discover` needs to act on this chain again
    /// later, out of band from the `LandingQueueEntry` that first observed
    /// the conflict — an authorized [`crate::landing::LandingPipeline::dispatch_held_conflict`]
    /// call only has the marker to reconstruct this context from, not the
    /// original queue entry.
    pub(crate) repo_path: String,
    /// Source: the branch that failed to merge, and the correction's base.
    pub(crate) branch: String,
    /// Exact source head at conflict time; never silently retargeted to a
    /// moved branch.
    pub(crate) head_sha: String,
    pub(crate) target: String,
    /// Exact target head at conflict time.
    pub(crate) target_head: String,
    /// `git merge-base(branch, target)` — where the two histories diverged.
    pub(crate) fork_point: String,
    pub(crate) task: String,
    pub(crate) rework_ticket: String,
    /// Bounded (see [`bound_conflict_detail`]) conflict report from git.
    pub(crate) conflict_detail: String,
    /// Blast radius: branch vs. target diff, computed even though the merge
    /// itself never built.
    pub(crate) diff_files: u64,
    pub(crate) diff_lines: u64,
}

impl ConflictContext {
    /// Full identity, including both original and correction tickets.
    pub(crate) fn dispatch_key(&self) -> String {
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            self.repo, self.branch, self.head_sha, self.target, self.task, self.rework_ticket
        )
    }

    /// Pin the correction to the conflicted head and quote git's report verbatim.
    pub(crate) fn prompt(&self) -> String {
        format!(
            "The landing pipeline could not merge `{branch}` (source, exact head {head_sha}) \
             into `{target}` (exact head {target_head}) — a merge CONFLICT, not a reviewer \
             verdict; the candidate never even built. You are the bounded correction for that \
             conflict; this ticket is {rework_ticket}.\n\n\
             YOUR BASE IS `{branch}` AT {head_sha} — not {target}. Your worktree is already cut \
             from it, so the branch's own work is present. Merge or rebase `{target}` (fork \
             point {fork_point}) into `{branch}`, resolving every conflict without discarding \
             either side's intent. Do not force-push over history that predates this conflict \
             and do not redo work the branch already carries.\n\n\
             GIT'S CONFLICT REPORT (verbatim, bounded):\n{conflict_detail}\n\n\
             Scope: {diff_files} file(s) / {diff_lines} line(s) are in play between `{branch}` \
             and `{target}`. Resolve only the conflict; anything else you find is a \
             `rk ticket new`, not an edit.\n\n\
             Your branch lands back onto `{branch}`. Once it does, the landing pipeline \
             automatically resubmits the corrected `{branch}` to `{target}` for a fresh gate \
             run — so do NOT land, merge, or push to `{target}` yourself.",
            branch = self.branch,
            head_sha = self.head_sha,
            target = self.target,
            target_head = self.target_head,
            rework_ticket = self.rework_ticket,
            fork_point = self.fork_point,
            conflict_detail = self.conflict_detail,
            diff_files = self.diff_files,
            diff_lines = self.diff_lines,
        )
    }

    /// Evidence-rich human gate: evidence, decision, blast radius, and command.
    pub(crate) fn escalation(&self, withheld: &Withheld) -> String {
        format!(
            "steward: merge CONFLICT on {branch} for {task} was NOT auto-dispatched ({code}) — \
             branch held unmerged.\n\
             EVIDENCE: {branch} (source, head {head_sha}, fork point {fork_point}) failed to \
             merge into {target} (head {target_head}); {detail}. Git's conflict report: \
             {conflict_detail}\n\
             DECISION NEEDED: {decision}\n\
             BLAST RADIUS: {diff_files} file(s) / {diff_lines} line(s) on {branch}, held back \
             from {target}. Nothing merged; nothing mutated on {branch}.\n\
             RESOLVE WITH: rk spawn --repo {repo} --ticket {rework_ticket} --base {branch}   \
             (or `rk ticket update {rework_ticket} --status closed` to abandon the correction, \
             then resolve {branch} by hand and let the landing pipeline retry)",
            branch = self.branch,
            task = self.task,
            code = withheld.code,
            head_sha = self.head_sha,
            fork_point = self.fork_point,
            target = self.target,
            target_head = self.target_head,
            detail = withheld.detail,
            conflict_detail = self.conflict_detail,
            decision = withheld.decision,
            diff_files = self.diff_files,
            diff_lines = self.diff_lines,
            repo = self.repo,
            rework_ticket = self.rework_ticket,
        )
    }

    /// Durable marker payload — also the AC1 structured recovery item: it
    /// names the repository, source, target, fork point, exact heads, and
    /// bounded conflict evidence together in one place, so a peer reading
    /// `rk scan event landing_conflict_rework_dispatch` needs nothing else.
    pub(crate) fn marker_payload(&self, attempt: u32, agent: Option<&str>, state: &str) -> Value {
        json!({
            "dispatch_key": self.dispatch_key(),
            "repo": self.repo,
            "repo_path": self.repo_path,
            "source": self.branch,
            "branch": self.branch,
            "head_sha": self.head_sha,
            "target": self.target,
            "target_head": self.target_head,
            "fork_point": self.fork_point,
            "task": self.task,
            "rework_ticket": self.rework_ticket,
            "conflict_evidence": self.conflict_detail,
            "agent": agent,
            "attempt": attempt,
            "state": state,
            "diff_files": self.diff_files,
            "diff_lines": self.diff_lines,
        })
    }

    /// Reconstruct a context from its own [`Self::marker_payload`] — the
    /// inverse used by [`crate::landing::LandingPipeline::dispatch_held_conflict`],
    /// which only has the durable marker (not the original
    /// `LandingQueueEntry`) to act from once an orchestrator decision
    /// authorizes the dispatch. `None` if any required field is absent or
    /// the wrong shape — a corrupt or foreign marker must never be silently
    /// coerced into a chain to dispatch against.
    pub(crate) fn from_marker_payload(payload: &Value) -> Option<Self> {
        let field = |key: &str| payload.get(key).and_then(Value::as_str).map(str::to_string);
        Some(Self {
            repo: field("repo")?,
            repo_path: field("repo_path")?,
            branch: field("branch")?,
            head_sha: field("head_sha")?,
            target: field("target")?,
            target_head: field("target_head")?,
            fork_point: field("fork_point")?,
            task: field("task")?,
            rework_ticket: field("rework_ticket")?,
            conflict_detail: field("conflict_evidence")?,
            diff_files: payload.get("diff_files").and_then(Value::as_u64)?,
            diff_lines: payload.get("diff_lines").and_then(Value::as_u64)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::Authority;

    fn evidence(detail: &str) -> ConflictEvidence<'_> {
        ConflictEvidence {
            conflict_detail: detail,
            protected_path_hit: false,
            diff_files: 3,
            diff_lines: 42,
            max_diff_files: 50,
            max_diff_lines: 2000,
        }
    }

    fn ctx() -> ConflictContext {
        ConflictContext {
            repo: "code-repo".into(),
            repo_path: "/repos/code-repo".into(),
            branch: "feature".into(),
            head_sha: "abc123".into(),
            target: "main".into(),
            target_head: "def456".into(),
            fork_point: "111000".into(),
            task: "TKT-1".into(),
            rework_ticket: "TKT-2".into(),
            conflict_detail: "CONFLICT (content): Merge conflict in src.rs".into(),
            diff_files: 3,
            diff_lines: 42,
        }
    }

    #[test]
    fn bound_conflict_detail_trims_and_caps() {
        assert_eq!(bound_conflict_detail("  hello  "), "hello");
        let long = "x".repeat(CONFLICT_DETAIL_MAX_CHARS + 50);
        let bounded = bound_conflict_detail(&long);
        assert!(bounded.ends_with("… (truncated)"));
        assert!(bounded.chars().count() < long.chars().count());
    }

    #[test]
    fn classifier_holds_unreadable_protected_or_oversized_conflicts_for_a_human() {
        let cases: [(ConflictEvidence, Authority, Option<&str>); 4] = [
            (evidence("CONFLICT in a.rs"), Authority::Orchestrator, None),
            (
                evidence("   "),
                Authority::Human,
                Some("unbounded-conflict-evidence"),
            ),
            (
                ConflictEvidence {
                    protected_path_hit: true,
                    ..evidence("CONFLICT in .rk/repo.cue")
                },
                Authority::Human,
                Some("protected-path-impact"),
            ),
            (
                ConflictEvidence {
                    diff_files: 500,
                    ..evidence("CONFLICT in a.rs")
                },
                Authority::Human,
                Some("blast-radius-uncertain"),
            ),
        ];
        for (evidence, expected_authority, expected_code) in cases {
            let (authority, withheld) = classify_conflict_authority(&evidence);
            assert_eq!(authority, expected_authority, "evidence: {evidence:?}");
            assert_eq!(withheld.as_ref().map(|w| w.code), expected_code);
        }
    }

    #[test]
    fn policy_caps_are_hard_ceilings_and_zero_spend_is_unlimited() {
        let default = ConflictPolicy::default();
        assert!(default.auto_dispatch);
        let bounded = evidence("CONFLICT in a.rs");
        let cases = [
            (default.clone(), 0, 0.0, None),
            (
                default.clone(),
                default.max_attempts,
                0.0,
                Some("attempts-exhausted"),
            ),
            (
                ConflictPolicy {
                    max_usd: 5,
                    ..default.clone()
                },
                0,
                5.01,
                Some("budget-exhausted"),
            ),
            (
                ConflictPolicy {
                    max_usd: 0,
                    ..default.clone()
                },
                0,
                9_999.0,
                None,
            ),
            (
                ConflictPolicy {
                    auto_dispatch: false,
                    ..default
                },
                0,
                0.0,
                Some("auto-dispatch-disabled"),
            ),
        ];
        for (policy, attempts, spent, expected_code) in cases {
            let actual = route(&policy, &bounded, attempts, spent);
            match (actual, expected_code) {
                (ReworkRoute::Dispatch { attempt: 1 }, None) => {}
                (ReworkRoute::Withhold(value), Some(code)) => assert_eq!(value.code, code),
                (actual, expected) => panic!("route {actual:?}, expected code {expected:?}"),
            }
        }
    }

    #[test]
    fn the_prompt_pins_the_conflicted_branch_and_both_exact_heads() {
        let prompt = ctx().prompt();
        assert!(
            prompt.contains("YOUR BASE IS `feature` AT abc123"),
            "{prompt}"
        );
        assert!(prompt.contains("not main"), "{prompt}");
        assert!(prompt.contains("exact head def456"), "{prompt}");
        assert!(prompt.contains("fork point 111000"), "{prompt}");
        assert!(prompt.contains("TKT-2"), "{prompt}");
        assert!(prompt.contains("Merge conflict in src.rs"), "{prompt}");
    }

    #[test]
    fn the_escalation_carries_evidence_decision_blast_radius_and_the_resolving_command() {
        let withheld = Withheld {
            code: "blast-radius-uncertain",
            detail: "too large".into(),
            decision: "judge it yourself".into(),
        };
        let text = ctx().escalation(&withheld);
        assert!(text.contains("EVIDENCE:"), "{text}");
        assert!(
            text.contains("DECISION NEEDED: judge it yourself"),
            "{text}"
        );
        assert!(
            text.contains("BLAST RADIUS: 3 file(s) / 42 line(s)"),
            "{text}"
        );
        assert!(
            text.contains("rk spawn --repo code-repo --ticket TKT-2 --base feature"),
            "{text}"
        );
        assert!(text.contains("fork point 111000"), "{text}");
    }

    #[test]
    fn the_ticket_coalesce_key_is_namespaced_apart_from_review_rework() {
        let key = ticket_coalesce_key("code-repo", "feature", "abc123", "main", "TKT-1");
        assert_eq!(
            key,
            "landing-conflict-rework:code-repo:feature:abc123:main:TKT-1"
        );
        assert_ne!(
            key,
            crate::landing_rework::ticket_coalesce_key(
                "code-repo",
                "feature",
                "abc123",
                "main",
                "TKT-1"
            )
        );
    }

    #[test]
    fn dispatch_key_includes_original_and_correction_ticket() {
        let key = ctx().dispatch_key();
        for component in ["code-repo", "feature", "abc123", "main", "TKT-1", "TKT-2"] {
            assert!(key.split('\0').any(|part| part == component), "{key:?}");
        }
    }

    #[test]
    fn marker_payload_carries_the_full_structured_recovery_item() {
        let payload = ctx().marker_payload(1, Some("Whisker"), "dispatching");
        for (key, expected) in [
            ("repo", "code-repo"),
            ("source", "feature"),
            ("target", "main"),
            ("fork_point", "111000"),
            ("head_sha", "abc123"),
            ("target_head", "def456"),
        ] {
            assert_eq!(payload[key], expected, "{key}");
        }
        assert!(payload["conflict_evidence"]
            .as_str()
            .unwrap()
            .contains("Merge conflict in src.rs"));
        assert_eq!(payload["repo_path"], "/repos/code-repo");
    }

    #[test]
    fn from_marker_payload_round_trips_marker_payload() {
        let original = ctx();
        let payload = original.marker_payload(2, Some("Whisker"), "dispatching");
        let reconstructed =
            ConflictContext::from_marker_payload(&payload).expect("payload has every field");
        assert_eq!(reconstructed.repo, original.repo);
        assert_eq!(reconstructed.repo_path, original.repo_path);
        assert_eq!(reconstructed.branch, original.branch);
        assert_eq!(reconstructed.head_sha, original.head_sha);
        assert_eq!(reconstructed.target, original.target);
        assert_eq!(reconstructed.target_head, original.target_head);
        assert_eq!(reconstructed.fork_point, original.fork_point);
        assert_eq!(reconstructed.task, original.task);
        assert_eq!(reconstructed.rework_ticket, original.rework_ticket);
        assert_eq!(reconstructed.conflict_detail, original.conflict_detail);
        assert_eq!(reconstructed.diff_files, original.diff_files);
        assert_eq!(reconstructed.diff_lines, original.diff_lines);
    }

    #[test]
    fn from_marker_payload_refuses_a_marker_missing_a_required_field() {
        let mut payload = ctx().marker_payload(1, None, "dispatching");
        payload.as_object_mut().unwrap().remove("repo_path");
        assert!(ConflictContext::from_marker_payload(&payload).is_none());
    }
}
