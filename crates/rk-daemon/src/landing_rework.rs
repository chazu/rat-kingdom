//! Unattended REWORK routing for the daemon-native landing pipeline.
//!
//! Before this module, a reviewer REWORK verdict was a full stop: the
//! pipeline preserved the reviewed branch, filed a rework ticket
//! (`LandingPipeline::file_rework_ticket`), and returned. Nothing dispatched
//! the ticket, so a human — or a supervising LLM polling `rk ticket list` —
//! had to notice it and spawn an agent by hand, even when the reviewer had
//! supplied a precise, bounded correction (the live case that motivated this:
//! TKT-01M0E8PMWCRKE6WZ4ZNYB6Y6YS filed TKT-01M0ECVY5NVMRC7WQQK1S1ZAAG and
//! then sat).
//!
//! The decision this module owns is deliberately narrow, and it is a
//! *classification*, not a judgment about the code: does the verdict describe
//! **delegated-LLM work** (a bounded correction an agent can carry out from
//! the reviewer's own notes) or **human authority** (a product/policy call, or
//! a verdict too vague to act on)? That vocabulary is the existing authority
//! ladder's ([`crate::reconcile::Authority`], `crate::authority`), reused
//! rather than reinvented: `Orchestrator` means "the fleet may resolve this
//! unattended", `Human` means "one explicit gate, no dispatch".
//!
//! Conservative by construction, in the same spirit as [`crate::authority`]:
//!
//! * the classifier FAILS CLOSED — anything it cannot positively recognize as
//!   a bounded correction is [`Authority::Human`], so a reviewer that writes
//!   nothing useful, or explicitly asks for a human, never triggers a spawn;
//! * a repo may narrow the policy ([`ReworkPolicy`]) toward "never dispatch"
//!   but the caps are hard ceilings, checked before every dispatch;
//! * the *withheld* path is not silent. It raises exactly one durable
//!   escalation carrying evidence, the decision being asked for, the blast
//!   radius, and the exact command that resolves it — see
//!   [`Withheld::escalation`].
//!
//! Everything here is pure: no `Space`, no git, no spawn. The pipeline side
//! (durable dispatch markers, the spawn itself, and the parent-branch
//! resubmission after a rework lands) lives in [`crate::landing`], which is
//! where the idempotency guarantees are enforced.

use crate::reconcile::Authority;
use serde_json::{json, Value};

/// Identity of the durable per-`(branch, head_sha)` marker recording that a
/// REWORK verdict was routed — written by the pipeline BEFORE it spawns, and
/// probed before it routes, so a redelivered completion, a daemon restart, or
/// a repeated queue scan can never produce a second rework ticket or a second
/// rework agent for the same reviewed commit. `Furniture`, scoped to the repo,
/// keyed the same way the review artifact itself is
/// (`Pattern::for_commit`).
pub(crate) const REWORK_DISPATCH_IDENTITY: &str = "landing_rework_dispatch";

/// Stable dedupe key for the rework ticket itself. Target and original task
/// are part of the key: the same branch/head submitted to a different target
/// or under a different ticket is a different review chain, not a replay.
///
/// A free function rather than a [`ReworkContext`] method because the pipeline
/// files the ticket BEFORE it can build a context (the context needs the
/// ticket's own identity), so the key has to be derivable from the work key
/// alone.
pub(crate) fn ticket_coalesce_key(
    repo: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    task: &str,
) -> String {
    format!("landing-rework:{repo}:{branch}:{head_sha}:{target}:{task}")
}

/// Per-repository bounds on unattended rework, resolved from
/// `rk_workflow::LandingPolicy` (`.rk/repo.cue`, digest-activated like every
/// other repo policy) — the landing pipeline's `GateConfig` counterpart for
/// the REWORK arm.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReworkPolicy {
    /// Master switch. `false` restores the pre-feature behavior exactly: file
    /// the ticket, hold the branch, escalate nothing extra.
    pub(crate) auto_dispatch: bool,
    /// How many rework agents may be dispatched for one reviewed branch
    /// across its whole review→rework→re-review chain. `0` disables dispatch
    /// as surely as `auto_dispatch: false` does.
    pub(crate) max_attempts: u32,
    /// Cumulative USD across the original agent and every rework agent in
    /// this chain. `0` means unlimited, matching the fleet budget convention
    /// (`rk_ledger::Budget`). Whole dollars: a rework ceiling is a
    /// coarse "is this still cheap automation" question, and an integer keeps
    /// `RepositoryPolicy` `Eq` (it is compared for activation-digest
    /// equality), which a float field would silently take away.
    pub(crate) max_usd: u32,
}

impl ReworkPolicy {
    pub(crate) fn from_landing(policy: &rk_workflow::LandingPolicy) -> Self {
        Self {
            auto_dispatch: policy.rework_auto_dispatch,
            max_attempts: policy.max_rework_attempts,
            max_usd: policy.rework_max_usd,
        }
    }
}

impl Default for ReworkPolicy {
    fn default() -> Self {
        Self::from_landing(&rk_workflow::LandingPolicy::default())
    }
}

/// Why a REWORK verdict was NOT dispatched unattended. Carries a stable
/// machine-readable `code` (so `rk inbox`/analytics can group these without
/// parsing prose) alongside the human-facing `detail` and the decision the
/// escalation is actually asking a human to make.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Withheld {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
    pub(crate) decision: String,
}

/// The routing decision for one REWORK verdict.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReworkRoute {
    /// Delegated-LLM work, inside every cap: dispatch exactly one rework
    /// agent. Carries the 1-based attempt number this dispatch will be.
    Dispatch { attempt: u32 },
    /// Human authority, or a cap/budget exhausted: file the ticket, hold the
    /// branch, and raise one durable escalation.
    Withhold(Withheld),
}

/// Where a REWORK verdict sits on the authority ladder.
///
/// FAIL-CLOSED: only a verdict that positively looks like a bounded
/// correction earns [`Authority::Orchestrator`]. Recognized signals, in
/// precedence order:
///
/// 1. an explicit `authority` field on the review artifact — a reviewer may
///    always escalate itself by writing `"authority": "human"`, and may never
///    widen past `orchestrator` (an unrecognized value is treated as `human`,
///    not ignored, so a typo cannot read as consent);
/// 2. `decision_required: true` or `bounded: false` — the reviewer saying in
///    structured form that this is a product/policy call;
/// 3. otherwise, non-empty `notes`: something concrete for an agent to act
///    on. Empty or whitespace-only notes are unbounded by definition — there
///    is nothing to hand a rework agent — so they escalate.
pub(crate) fn classify_authority(review: Option<&Value>) -> (Authority, Option<Withheld>) {
    let Some(review) = review else {
        return (
            Authority::Human,
            Some(Withheld {
                code: "no-review-artifact",
                detail: "the REWORK verdict has no readable review artifact, so there are no \
                         reviewer notes to hand a rework agent"
                    .into(),
                decision: "read the branch yourself and either dispatch a rework agent with \
                           explicit instructions, or land/abandon the branch"
                    .into(),
            }),
        );
    };
    if let Some(raw) = review.get("authority").and_then(Value::as_str) {
        // `mechanical` is not a thing a reviewer can ask for here — a rework
        // is by definition an LLM edit — so it maps to `orchestrator` rather
        // than being rejected as unknown.
        let declared = match raw.trim() {
            "orchestrator" | "mechanical" | "delegated" | "agent" | "llm" => {
                Some(Authority::Orchestrator)
            }
            "human" | "operator" => Some(Authority::Human),
            _ => None,
        };
        match declared {
            Some(Authority::Human) => {
                return (
                    Authority::Human,
                    Some(Withheld {
                        code: "reviewer-declared-human",
                        detail: format!(
                            "the reviewer classified its own REWORK as human authority \
                             (authority: {raw:?})"
                        ),
                        decision: "make the call the reviewer refused to make, then dispatch or \
                                   abandon"
                            .into(),
                    }),
                );
            }
            None => {
                return (
                    Authority::Human,
                    Some(Withheld {
                        code: "unrecognized-authority",
                        detail: format!(
                            "the review artifact declares an authority this build does not \
                             recognize ({raw:?}) — treated as human rather than ignored, so a \
                             typo cannot read as consent to dispatch"
                        ),
                        decision: "correct the reviewer's verdict, then dispatch or abandon".into(),
                    }),
                );
            }
            Some(_) => {}
        }
    }
    if review.get("decision_required").and_then(Value::as_bool) == Some(true)
        || review.get("bounded").and_then(Value::as_bool) == Some(false)
    {
        return (
            Authority::Human,
            Some(Withheld {
                code: "ambiguous-decision",
                detail: "the reviewer flagged this REWORK as an open product or policy decision \
                         rather than a bounded correction"
                    .into(),
                decision: "decide the product/policy question, then dispatch or abandon".into(),
            }),
        );
    }
    if notes(Some(review)).is_empty() {
        return (
            Authority::Human,
            Some(Withheld {
                code: "unbounded-verdict",
                detail: "the REWORK verdict carries no notes, so the correction is not bounded — \
                         there is nothing to hand a rework agent"
                    .into(),
                decision: "read the branch yourself and either dispatch a rework agent with \
                           explicit instructions, or land/abandon the branch"
                    .into(),
            }),
        );
    }
    (Authority::Orchestrator, None)
}

/// The reviewer's notes, trimmed. Empty when the artifact has none — the
/// signal [`classify_authority`] treats as "unbounded".
pub(crate) fn notes(review: Option<&Value>) -> String {
    review
        .and_then(|r| r.get("notes"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Decide what to do with one REWORK verdict.
///
/// `attempts_used` is how many rework agents this reviewed branch has ALREADY
/// had dispatched (counted from durable markers, not from memory);
/// `spent_usd` is the cumulative cost of the original agent plus every rework
/// agent in the chain. Both caps are checked AFTER the authority
/// classification so an escalation always names the most specific reason: a
/// verdict that is both human-authority and over budget reads better as the
/// former.
pub(crate) fn route(
    policy: &ReworkPolicy,
    review: Option<&Value>,
    attempts_used: u32,
    spent_usd: f64,
) -> ReworkRoute {
    if !policy.auto_dispatch {
        return ReworkRoute::Withhold(Withheld {
            code: "auto-dispatch-disabled",
            detail: "this repository's landing policy has reworkAutoDispatch disabled".into(),
            decision: "dispatch the rework ticket by hand, or enable reworkAutoDispatch in \
                       .rk/repo.cue"
                .into(),
        });
    }
    if let (Authority::Human, Some(withheld)) = classify_authority(review) {
        return ReworkRoute::Withhold(withheld);
    }
    if attempts_used >= policy.max_attempts {
        return ReworkRoute::Withhold(Withheld {
            code: "attempts-exhausted",
            detail: format!(
                "this branch has already had {attempts_used} automatic rework attempt(s), at or \
                 over the repository cap of {}",
                policy.max_attempts
            ),
            decision: "decide whether this work is converging: dispatch another rework by hand, \
                       land it, or abandon the branch"
                .into(),
        });
    }
    if policy.max_usd > 0 && spent_usd >= f64::from(policy.max_usd) {
        return ReworkRoute::Withhold(Withheld {
            code: "budget-exhausted",
            detail: format!(
                "this branch's review/rework chain has already spent ${spent_usd:.2}, at or over \
                 the repository cap of ${}",
                policy.max_usd
            ),
            decision: "decide whether this work is worth more spend: dispatch another rework by \
                       hand, land it, or abandon the branch"
                .into(),
        });
    }
    ReworkRoute::Dispatch {
        attempt: attempts_used + 1,
    }
}

/// Everything a rework dispatch or escalation needs to name itself. Built by
/// the pipeline from the queue entry, the review artifact, and a git
/// diff-stat.
#[derive(Debug, Clone)]
pub(crate) struct ReworkContext {
    pub(crate) repo: String,
    /// The REVIEWED branch — the rework agent's base, and the target its own
    /// branch lands back onto.
    pub(crate) branch: String,
    /// The exact reviewed head. A rework must start from this commit, not
    /// from the branch name (which may have moved) and never from `main`.
    pub(crate) head_sha: String,
    /// Where the reviewed branch was headed before the REWORK — where it is
    /// resubmitted once the rework lands into it.
    pub(crate) target: String,
    /// The original ticket the reviewed branch was delivering.
    pub(crate) task: String,
    /// The follow-up ticket filed for this verdict.
    pub(crate) rework_ticket: String,
    pub(crate) notes: String,
    /// Changed files / changed lines of the reviewed branch against its
    /// target — the blast radius an escalation must state.
    pub(crate) diff_files: u64,
    pub(crate) diff_lines: u64,
}

impl ReworkContext {
    /// Full logical dispatch identity. The follow-up ticket is included even
    /// though it is itself coalesced from the preceding fields: persisting all
    /// six dimensions makes replay evidence self-contained and prevents a
    /// malformed/stale ticket association from being treated as the same
    /// dispatch.
    pub(crate) fn dispatch_key(&self) -> String {
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            self.repo, self.branch, self.head_sha, self.target, self.task, self.rework_ticket
        )
    }

    /// The rework agent's task prompt. States the identities on both ends
    /// (original and rework ticket), pins the exact base commit, quotes the
    /// reviewer verbatim, and — critically — tells the agent it is landing
    /// back onto the REVIEWED branch, not onto the original target: the
    /// pipeline resubmits the corrected parent itself once this lands.
    pub(crate) fn prompt(&self) -> String {
        format!(
            "A reviewer returned REWORK on branch `{branch}` (exact head {head_sha}), which was \
             delivering {task}. You are the bounded correction for that verdict; this ticket is \
             {rework_ticket}.\n\n\
             YOUR BASE IS `{branch}` AT {head_sha} — not {target}. Your worktree is already cut \
             from it, so the reviewed work is present and `git log {target}..HEAD` shows it. Do \
             not rebase onto {target} and do not re-do the original work: fix only what the \
             reviewer named.\n\n\
             REVIEWER NOTES (verbatim):\n{notes}\n\n\
             Scope: {diff_files} file(s) / {diff_lines} line(s) were under review. Address every \
             point above and nothing else; anything you find that is out of scope is a \
             `rk ticket new`, not an edit.\n\n\
             Your branch lands back onto `{branch}`. Once it does, the landing pipeline \
             automatically resubmits the corrected `{branch}` to `{target}` for a fresh review — \
             so do NOT land, merge, or push to `{target}` yourself.",
            branch = self.branch,
            head_sha = self.head_sha,
            task = self.task,
            rework_ticket = self.rework_ticket,
            target = self.target,
            notes = self.notes,
            diff_files = self.diff_files,
            diff_lines = self.diff_lines,
        )
    }

    /// The escalation text for a withheld dispatch — one durable `rk inbox`
    /// row carrying the four things a human needs to act without reopening
    /// the whole case: the EVIDENCE (who said what, about which commit), the
    /// DECISION being asked for, the BLAST RADIUS, and the EXACT command that
    /// resolves it.
    pub(crate) fn escalation(&self, withheld: &Withheld) -> String {
        format!(
            "steward: REWORK on {branch} for {task} was NOT auto-dispatched ({code}) — branch \
             held unmerged.\n\
             EVIDENCE: reviewer verdict REWORK at {head_sha}; {detail}. Reviewer notes: {notes}\n\
             DECISION NEEDED: {decision}\n\
             BLAST RADIUS: {diff_files} file(s) / {diff_lines} line(s) on {branch}, held back \
             from {target}. Nothing merged.\n\
             RESOLVE WITH: rk spawn --repo {repo} --ticket {rework_ticket} --base {branch}   \
             (or `rk ticket update {rework_ticket} --status closed` to abandon the correction, \
             then `rk land {branch} --target {target}` to land as-is)",
            branch = self.branch,
            task = self.task,
            code = withheld.code,
            head_sha = self.head_sha,
            detail = withheld.detail,
            notes = if self.notes.is_empty() {
                "(none recorded)"
            } else {
                &self.notes
            },
            decision = withheld.decision,
            diff_files = self.diff_files,
            diff_lines = self.diff_lines,
            target = self.target,
            repo = self.repo,
            rework_ticket = self.rework_ticket,
        )
    }

    /// The durable dispatch marker's payload. `branch`/`head_sha` are at the
    /// top level and spelled exactly so because `Pattern::for_commit` matches
    /// on those two keys — this is what makes the marker probe a commit-keyed
    /// lookup rather than a full scan.
    pub(crate) fn marker_payload(&self, attempt: u32, agent: Option<&str>, state: &str) -> Value {
        json!({
            "dispatch_key": self.dispatch_key(),
            "branch": self.branch,
            "head_sha": self.head_sha,
            "repo": self.repo,
            "target": self.target,
            "task": self.task,
            "rework_ticket": self.rework_ticket,
            "agent": agent,
            "attempt": attempt,
            "state": state,
            "diff_files": self.diff_files,
            "diff_lines": self.diff_lines,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn review(extra: Value) -> Value {
        let mut base = json!({
            "task": "TKT-1",
            "recommendation": "REWORK",
            "notes": "the workflow-instance ledger input is missing from reconcile",
            "branch": "feature",
            "head_sha": "abc123",
        });
        for (k, v) in extra.as_object().unwrap() {
            base[k] = v.clone();
        }
        base
    }

    fn ctx() -> ReworkContext {
        ReworkContext {
            repo: "code-repo".into(),
            branch: "feature".into(),
            head_sha: "abc123".into(),
            target: "main".into(),
            task: "TKT-1".into(),
            rework_ticket: "TKT-2".into(),
            notes: "add the ledger input".into(),
            diff_files: 3,
            diff_lines: 42,
        }
    }

    #[test]
    fn a_bounded_verdict_with_notes_is_delegated() {
        let (authority, withheld) = classify_authority(Some(&review(json!({}))));
        assert_eq!(authority, Authority::Orchestrator);
        assert!(withheld.is_none());
    }

    #[test]
    fn a_verdict_with_no_notes_is_unbounded_and_escalates() {
        let (authority, withheld) = classify_authority(Some(&review(json!({"notes": "   "}))));
        assert_eq!(authority, Authority::Human);
        assert_eq!(withheld.unwrap().code, "unbounded-verdict");
    }

    #[test]
    fn a_missing_review_artifact_escalates() {
        let (authority, withheld) = classify_authority(None);
        assert_eq!(authority, Authority::Human);
        assert_eq!(withheld.unwrap().code, "no-review-artifact");
    }

    #[test]
    fn a_reviewer_may_escalate_itself() {
        let (authority, withheld) =
            classify_authority(Some(&review(json!({"authority": "human"}))));
        assert_eq!(authority, Authority::Human);
        assert_eq!(withheld.unwrap().code, "reviewer-declared-human");
    }

    /// A typo must not read as consent — the whole fail-closed point.
    #[test]
    fn an_unrecognized_authority_value_escalates_rather_than_being_ignored() {
        let (authority, withheld) =
            classify_authority(Some(&review(json!({"authority": "orchestratr"}))));
        assert_eq!(authority, Authority::Human);
        assert_eq!(withheld.unwrap().code, "unrecognized-authority");
    }

    #[test]
    fn an_open_product_decision_escalates_even_with_notes() {
        for flag in [
            json!({"decision_required": true}),
            json!({"bounded": false}),
        ] {
            let (authority, withheld) = classify_authority(Some(&review(flag.clone())));
            assert_eq!(authority, Authority::Human, "flag: {flag}");
            assert_eq!(withheld.unwrap().code, "ambiguous-decision");
        }
    }

    #[test]
    fn the_default_policy_dispatches_one_bounded_attempt() {
        let policy = ReworkPolicy::default();
        assert!(policy.auto_dispatch);
        assert_eq!(
            route(&policy, Some(&review(json!({}))), 0, 0.0),
            ReworkRoute::Dispatch { attempt: 1 }
        );
    }

    #[test]
    fn the_attempt_cap_is_a_hard_ceiling() {
        let policy = ReworkPolicy::default();
        let used = policy.max_attempts;
        let ReworkRoute::Withhold(withheld) = route(&policy, Some(&review(json!({}))), used, 0.0)
        else {
            panic!("expected the attempt cap to withhold at {used} attempts");
        };
        assert_eq!(withheld.code, "attempts-exhausted");
    }

    #[test]
    fn the_spend_cap_is_a_hard_ceiling_and_zero_means_unlimited() {
        let capped = ReworkPolicy {
            max_usd: 5,
            ..ReworkPolicy::default()
        };
        let ReworkRoute::Withhold(withheld) = route(&capped, Some(&review(json!({}))), 0, 5.01)
        else {
            panic!("expected the spend cap to withhold");
        };
        assert_eq!(withheld.code, "budget-exhausted");

        let unlimited = ReworkPolicy {
            max_usd: 0,
            ..ReworkPolicy::default()
        };
        assert_eq!(
            route(&unlimited, Some(&review(json!({}))), 0, 9_999.0),
            ReworkRoute::Dispatch { attempt: 1 }
        );
    }

    #[test]
    fn disabling_auto_dispatch_restores_the_old_hold() {
        let policy = ReworkPolicy {
            auto_dispatch: false,
            ..ReworkPolicy::default()
        };
        let ReworkRoute::Withhold(withheld) = route(&policy, Some(&review(json!({}))), 0, 0.0)
        else {
            panic!("expected a disabled policy to withhold");
        };
        assert_eq!(withheld.code, "auto-dispatch-disabled");
    }

    /// The prompt must pin the exact base — the acceptance criterion that
    /// distinguishes a rework from "re-run the ticket against main".
    #[test]
    fn the_prompt_pins_the_reviewed_branch_and_exact_head() {
        let prompt = ctx().prompt();
        assert!(
            prompt.contains("YOUR BASE IS `feature` AT abc123"),
            "{prompt}"
        );
        assert!(prompt.contains("not main"), "{prompt}");
        assert!(prompt.contains("TKT-1"), "{prompt}");
        assert!(prompt.contains("TKT-2"), "{prompt}");
        assert!(prompt.contains("add the ledger input"), "{prompt}");
    }

    #[test]
    fn the_escalation_carries_evidence_decision_blast_radius_and_the_resolving_command() {
        let withheld = Withheld {
            code: "unbounded-verdict",
            detail: "no notes".into(),
            decision: "read the branch yourself".into(),
        };
        let text = ctx().escalation(&withheld);
        assert!(text.contains("EVIDENCE:"), "{text}");
        assert!(
            text.contains("DECISION NEEDED: read the branch yourself"),
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
    }

    #[test]
    fn the_ticket_coalesce_key_names_the_whole_review_chain() {
        assert_eq!(
            ticket_coalesce_key("code-repo", "feature", "abc123", "main", "TKT-1"),
            "landing-rework:code-repo:feature:abc123:main:TKT-1"
        );
    }

    #[test]
    fn dispatch_key_includes_original_and_rework_ticket() {
        let key = ctx().dispatch_key();
        for component in ["code-repo", "feature", "abc123", "main", "TKT-1", "TKT-2"] {
            assert!(key.split('\0').any(|part| part == component), "{key:?}");
        }
    }
}
