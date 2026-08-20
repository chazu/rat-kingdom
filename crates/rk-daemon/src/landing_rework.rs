//! Pure policy and evidence formatting for unattended reviewer REWORK.
//!
//! Only a positively identified bounded correction receives orchestrator
//! authority; missing, ambiguous, or human-declared work fails closed. Repo
//! policy can narrow that authority further with attempt and spend ceilings.
//! Durable markers, spawning, and corrected-parent resubmission remain in
//! [`crate::landing`].

use crate::reconcile::Authority;
use serde_json::{json, Value};

/// Durable, repo-scoped routing marker written before a rework spawn.
pub(crate) const REWORK_DISPATCH_IDENTITY: &str = "landing_rework_dispatch";

/// Stable ticket dedupe key. Target and task distinguish separate review chains.
pub(crate) fn ticket_coalesce_key(
    repo: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    task: &str,
) -> String {
    format!("landing-rework:{repo}:{branch}:{head_sha}:{target}:{task}")
}

/// Activated repository bounds for unattended rework.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReworkPolicy {
    /// `false` files the ticket and holds the branch without dispatching.
    pub(crate) auto_dispatch: bool,
    /// Hard ceiling across a review→rework→re-review chain.
    pub(crate) max_attempts: u32,
    /// Whole-dollar chain ceiling; `0` means unlimited and preserves policy `Eq`.
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

/// Machine-readable reason and evidence for a withheld dispatch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Withheld {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
    pub(crate) decision: String,
}

fn human(
    code: &'static str,
    detail: impl Into<String>,
    decision: impl Into<String>,
) -> (Authority, Option<Withheld>) {
    (
        Authority::Human,
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

/// The routing decision for one REWORK verdict.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReworkRoute {
    /// Dispatch exactly one rework agent at this 1-based attempt.
    Dispatch { attempt: u32 },
    /// File the ticket, hold the branch, and raise one durable escalation.
    Withhold(Withheld),
}

/// Classify a verdict on the authority ladder, failing closed. Explicit
/// authority wins, decision flags come next, and non-empty notes are required.
pub(crate) fn classify_authority(review: Option<&Value>) -> (Authority, Option<Withheld>) {
    let Some(review) = review else {
        return human(
            "no-review-artifact",
            "the REWORK verdict has no readable review artifact, so there are no reviewer notes \
             to hand a rework agent",
            "read the branch yourself and either dispatch a rework agent with explicit \
             instructions, or land/abandon the branch",
        );
    };
    if let Some(raw) = review.get("authority").and_then(Value::as_str) {
        // Rework is an LLM edit, so `mechanical` maps to orchestrator.
        let declared = match raw.trim() {
            "orchestrator" | "mechanical" | "delegated" | "agent" | "llm" => {
                Some(Authority::Orchestrator)
            }
            "human" | "operator" => Some(Authority::Human),
            _ => None,
        };
        match declared {
            Some(Authority::Human) => {
                return human(
                    "reviewer-declared-human",
                    format!(
                        "the reviewer classified its own REWORK as human authority \
                         (authority: {raw:?})"
                    ),
                    "make the call the reviewer refused to make, then dispatch or abandon",
                );
            }
            None => {
                return human(
                    "unrecognized-authority",
                    format!(
                        "the review artifact declares an authority this build does not recognize \
                         ({raw:?}) — treated as human rather than ignored, so a typo cannot read \
                         as consent to dispatch"
                    ),
                    "correct the reviewer's verdict, then dispatch or abandon",
                );
            }
            Some(_) => {}
        }
    }
    if review.get("decision_required").and_then(Value::as_bool) == Some(true)
        || review.get("bounded").and_then(Value::as_bool) == Some(false)
    {
        return human(
            "ambiguous-decision",
            "the reviewer flagged this REWORK as an open product or policy decision rather than \
             a bounded correction",
            "decide the product/policy question, then dispatch or abandon",
        );
    }
    if notes(Some(review)).is_empty() {
        return human(
            "unbounded-verdict",
            "the REWORK verdict carries no notes, so the correction is not bounded — there is \
             nothing to hand a rework agent",
            "read the branch yourself and either dispatch a rework agent with explicit \
             instructions, or land/abandon the branch",
        );
    }
    (Authority::Orchestrator, None)
}

/// Trimmed reviewer notes; empty is unbounded.
pub(crate) fn notes(review: Option<&Value>) -> String {
    review
        .and_then(|r| r.get("notes"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Route one verdict. Authority precedes durable attempt and chain-spend caps.
pub(crate) fn route(
    policy: &ReworkPolicy,
    review: Option<&Value>,
    attempts_used: u32,
    spent_usd: f64,
) -> ReworkRoute {
    if !policy.auto_dispatch {
        return withhold(
            "auto-dispatch-disabled",
            "this repository's landing policy has reworkAutoDispatch disabled",
            "dispatch the rework ticket by hand, or enable reworkAutoDispatch in .rk/repo.cue",
        );
    }
    if let (Authority::Human, Some(withheld)) = classify_authority(review) {
        return ReworkRoute::Withhold(withheld);
    }
    if attempts_used >= policy.max_attempts {
        return withhold(
            "attempts-exhausted",
            format!(
                "this branch has already had {attempts_used} automatic rework attempt(s), at or \
                 over the repository cap of {}",
                policy.max_attempts
            ),
            "decide whether this work is converging: dispatch another rework by hand, land it, \
             or abandon the branch",
        );
    }
    if policy.max_usd > 0 && spent_usd >= f64::from(policy.max_usd) {
        return withhold(
            "budget-exhausted",
            format!(
                "this branch's review/rework chain has already spent ${spent_usd:.2}, at or over \
                 the repository cap of ${}",
                policy.max_usd
            ),
            "decide whether this work is worth more spend: dispatch another rework by hand, land \
             it, or abandon the branch",
        );
    }
    ReworkRoute::Dispatch {
        attempt: attempts_used + 1,
    }
}

/// Exact review-chain identity and evidence used by dispatch and escalation.
#[derive(Debug, Clone)]
pub(crate) struct ReworkContext {
    pub(crate) repo: String,
    /// Reviewed branch: the correction's base and merge target.
    pub(crate) branch: String,
    /// Exact reviewed head; never silently retargeted to a moved branch.
    pub(crate) head_sha: String,
    /// Original target used when the corrected parent is resubmitted.
    pub(crate) target: String,
    pub(crate) task: String,
    pub(crate) rework_ticket: String,
    pub(crate) notes: String,
    /// Reviewed branch blast radius against its target.
    pub(crate) diff_files: u64,
    pub(crate) diff_lines: u64,
}

impl ReworkContext {
    /// Full identity, including both original and correction tickets.
    pub(crate) fn dispatch_key(&self) -> String {
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            self.repo, self.branch, self.head_sha, self.target, self.task, self.rework_ticket
        )
    }

    /// Pin the correction to the reviewed head and quote the review verbatim.
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

    /// Evidence-rich human gate: evidence, decision, blast radius, and command.
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

    /// Durable marker payload; top-level branch/head fields support exact probes.
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
    fn classifier_delegates_only_bounded_verdicts_and_fails_closed() {
        let cases = [
            (Some(json!({})), Authority::Orchestrator, None),
            (
                Some(json!({"notes": "   "})),
                Authority::Human,
                Some("unbounded-verdict"),
            ),
            (None, Authority::Human, Some("no-review-artifact")),
            (
                Some(json!({"authority": "human"})),
                Authority::Human,
                Some("reviewer-declared-human"),
            ),
            // A typo must not read as consent.
            (
                Some(json!({"authority": "orchestratr"})),
                Authority::Human,
                Some("unrecognized-authority"),
            ),
            (
                Some(json!({"decision_required": true})),
                Authority::Human,
                Some("ambiguous-decision"),
            ),
            (
                Some(json!({"bounded": false})),
                Authority::Human,
                Some("ambiguous-decision"),
            ),
        ];
        for (extra, expected_authority, expected_code) in cases {
            let artifact = extra.map(review);
            let (authority, withheld) = classify_authority(artifact.as_ref());
            assert_eq!(authority, expected_authority, "artifact: {artifact:?}");
            assert_eq!(
                withheld.as_ref().map(|value| value.code),
                expected_code,
                "artifact: {artifact:?}"
            );
        }
    }

    #[test]
    fn policy_caps_are_hard_ceilings_and_zero_spend_is_unlimited() {
        let default = ReworkPolicy::default();
        assert!(default.auto_dispatch);
        let cases = [
            (default.clone(), 0, 0.0, None),
            (
                default.clone(),
                default.max_attempts,
                0.0,
                Some("attempts-exhausted"),
            ),
            (
                ReworkPolicy {
                    max_usd: 5,
                    ..default.clone()
                },
                0,
                5.01,
                Some("budget-exhausted"),
            ),
            (
                ReworkPolicy {
                    max_usd: 0,
                    ..default.clone()
                },
                0,
                9_999.0,
                None,
            ),
            (
                ReworkPolicy {
                    auto_dispatch: false,
                    ..default
                },
                0,
                0.0,
                Some("auto-dispatch-disabled"),
            ),
        ];
        let artifact = review(json!({}));
        for (policy, attempts, spent, expected_code) in cases {
            let actual = route(&policy, Some(&artifact), attempts, spent);
            match (actual, expected_code) {
                (ReworkRoute::Dispatch { attempt: 1 }, None) => {}
                (ReworkRoute::Withhold(value), Some(code)) => assert_eq!(value.code, code),
                (actual, expected) => panic!("route {actual:?}, expected code {expected:?}"),
            }
        }
    }

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
