//! Pure policy and evidence formatting for unattended reviewer-death retry.
//!
//! When a review workflow instance goes terminal (crashed, killed,
//! respawn-exhausted) WITHOUT ever producing a verdict artifact
//! (`ReviewWaitOutcome::ReviewerDied` — `crate::landing`), the death itself
//! carries no authority question to classify: unlike a REWORK verdict, there
//! are no reviewer notes to hand anyone, bounded or not. The only questions
//! are policy ones — is unattended retry enabled, and has this exact
//! candidate already spent its attempt/budget ceiling — so this module is
//! smaller than [`crate::landing_rework`], its direct template. Durable
//! markers, spawning, and instance-id derivation remain in [`crate::landing`].

use serde_json::{json, Value};

/// Durable, repo-scoped routing marker written before a replacement
/// reviewer is dispatched.
pub(crate) const REVIEW_DEATH_DISPATCH_IDENTITY: &str = "landing_review_death_dispatch";

/// Activated repository bounds for unattended review-death retry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ReviewDeathPolicy {
    /// `false` escalates to a human gate on the first death without dispatching.
    pub(crate) auto_retry: bool,
    /// Hard ceiling on replacement reviewers for one candidate's dead review.
    pub(crate) max_attempts: u32,
    /// Whole-dollar chain ceiling; `0` means unlimited and preserves policy `Eq`.
    pub(crate) max_usd: u32,
}

impl ReviewDeathPolicy {
    pub(crate) fn from_landing(policy: &rk_workflow::LandingPolicy) -> Self {
        Self {
            auto_retry: policy.review_death_auto_retry,
            max_attempts: policy.max_review_death_attempts,
            max_usd: policy.review_death_max_usd,
        }
    }
}

impl Default for ReviewDeathPolicy {
    fn default() -> Self {
        Self::from_landing(&rk_workflow::LandingPolicy::default())
    }
}

/// Machine-readable reason and evidence for a withheld retry.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Withheld {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
    pub(crate) decision: String,
}

fn withhold(
    code: &'static str,
    detail: impl Into<String>,
    decision: impl Into<String>,
) -> ReviewDeathRoute {
    ReviewDeathRoute::Withhold(Withheld {
        code,
        detail: detail.into(),
        decision: decision.into(),
    })
}

/// The routing decision for one dead-review outcome.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReviewDeathRoute {
    /// Dispatch exactly one replacement reviewer at this 1-based retry ordinal.
    Dispatch { attempt: u32 },
    /// Hold the branch and raise one durable human escalation.
    Withhold(Withheld),
}

/// Route one reviewer death. Policy is the whole ladder: there is no verdict
/// to classify, only whether unattended retry is enabled and whether this
/// exact candidate has already spent its attempt/budget ceiling.
pub(crate) fn route(
    policy: &ReviewDeathPolicy,
    attempts_used: u32,
    spent_usd: f64,
) -> ReviewDeathRoute {
    if !policy.auto_retry {
        return withhold(
            "auto-retry-disabled",
            "this repository's landing policy has reviewDeathAutoRetry disabled",
            "inspect the failed review and either record a fresh verdict or make the land \
             decision by hand",
        );
    }
    if attempts_used >= policy.max_attempts {
        return withhold(
            "attempts-exhausted",
            format!(
                "this candidate has already had {attempts_used} automatic replacement \
                 reviewer(s), at or over the repository cap of {}",
                policy.max_attempts
            ),
            "decide whether the reviewer keeps failing for an infrastructure reason or a real \
             one: retry by hand, record a verdict yourself, or make the land decision",
        );
    }
    if policy.max_usd > 0 && spent_usd >= f64::from(policy.max_usd) {
        return withhold(
            "budget-exhausted",
            format!(
                "this candidate's review-death retry chain has already spent ${spent_usd:.2}, at \
                 or over the repository cap of ${}",
                policy.max_usd
            ),
            "decide whether another reviewer is worth the spend: retry by hand, record a \
             verdict yourself, or make the land decision",
        );
    }
    ReviewDeathRoute::Dispatch {
        attempt: attempts_used + 1,
    }
}

/// Exact review-chain identity and evidence used by dispatch and escalation.
#[derive(Debug, Clone)]
pub(crate) struct ReviewDeathContext {
    pub(crate) repo: String,
    /// Filesystem checkout path accepted by `rk land --repo`.
    pub(crate) repo_path: String,
    /// Reviewed branch: the exact same candidate every retry re-reviews.
    pub(crate) branch: String,
    /// Exact reviewed head; a retry never silently retargets to a moved branch.
    pub(crate) head_sha: String,
    pub(crate) target: String,
    pub(crate) task: String,
}

impl ReviewDeathContext {
    /// Full identity, matching one candidate's whole retry chain.
    pub(crate) fn dispatch_key(&self) -> String {
        format!(
            "{}\0{}\0{}\0{}\0{}",
            self.repo, self.branch, self.head_sha, self.target, self.task
        )
    }

    /// Evidence-rich human gate: evidence, decision, and the resolving command.
    pub(crate) fn escalation(&self, withheld: &Withheld, death_context: &str) -> String {
        format!(
            "steward: review of {branch} for {task} died before a verdict and was NOT \
             automatically retried ({code}) — branch held unmerged.\n\
             EVIDENCE: exact reviewed head {head_sha}; the reviewer ended without producing a \
             verdict: {death_context}. {detail}\n\
             DECISION NEEDED: {decision}\n\
            RESOLVE WITH: rk land {branch} --repo {repo_path} --target {target} --task {task} \
             --force --reason 'human resolved {code}'",
            branch = self.branch,
            task = self.task,
            code = withheld.code,
            head_sha = self.head_sha,
            detail = withheld.detail,
            decision = withheld.decision,
            repo_path = self.repo_path,
            target = self.target,
        )
    }

    /// Durable marker payload; top-level branch/head fields support exact probes.
    pub(crate) fn marker_payload(&self, attempt: u32, instance_id: &str, state: &str) -> Value {
        json!({
            "dispatch_key": self.dispatch_key(),
            "branch": self.branch,
            "head_sha": self.head_sha,
            "repo": self.repo,
            "target": self.target,
            "task": self.task,
            "instance_id": instance_id,
            "attempt": attempt,
            "state": state,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> ReviewDeathContext {
        ReviewDeathContext {
            repo: "code-repo".into(),
            repo_path: "/checkouts/code-repo".into(),
            branch: "feature".into(),
            head_sha: "abc123".into(),
            target: "main".into(),
            task: "TKT-1".into(),
        }
    }

    #[test]
    fn policy_caps_are_hard_ceilings_and_zero_spend_is_unlimited() {
        let default = ReviewDeathPolicy::default();
        assert!(default.auto_retry);
        let cases = [
            (default, 0, 0.0, None),
            (
                default,
                default.max_attempts,
                0.0,
                Some("attempts-exhausted"),
            ),
            (
                ReviewDeathPolicy {
                    max_usd: 5,
                    ..default
                },
                0,
                5.01,
                Some("budget-exhausted"),
            ),
            (
                ReviewDeathPolicy {
                    max_usd: 0,
                    ..default
                },
                0,
                9_999.0,
                None,
            ),
            (
                ReviewDeathPolicy {
                    auto_retry: false,
                    ..default
                },
                0,
                0.0,
                Some("auto-retry-disabled"),
            ),
        ];
        for (policy, attempts, spent, expected_code) in cases {
            let actual = route(&policy, attempts, spent);
            match (actual, expected_code) {
                (ReviewDeathRoute::Dispatch { attempt: 1 }, None) => {}
                (ReviewDeathRoute::Withhold(value), Some(code)) => assert_eq!(value.code, code),
                (actual, expected) => panic!("route {actual:?}, expected code {expected:?}"),
            }
        }
    }

    #[test]
    fn successive_attempts_increment_off_attempts_used() {
        let policy = ReviewDeathPolicy {
            max_attempts: 3,
            ..ReviewDeathPolicy::default()
        };
        assert_eq!(
            route(&policy, 0, 0.0),
            ReviewDeathRoute::Dispatch { attempt: 1 }
        );
        assert_eq!(
            route(&policy, 1, 0.0),
            ReviewDeathRoute::Dispatch { attempt: 2 }
        );
        assert_eq!(
            route(&policy, 2, 0.0),
            ReviewDeathRoute::Dispatch { attempt: 3 }
        );
        assert!(matches!(
            route(&policy, 3, 0.0),
            ReviewDeathRoute::Withhold(_)
        ));
    }

    #[test]
    fn the_escalation_carries_evidence_decision_and_the_resolving_command() {
        let withheld = Withheld {
            code: "attempts-exhausted",
            detail: "cap reached".into(),
            decision: "retry by hand".into(),
        };
        let text = ctx().escalation(&withheld, "harness exited mid-verification");
        assert!(text.contains("EVIDENCE:"), "{text}");
        assert!(text.contains("harness exited mid-verification"), "{text}");
        assert!(text.contains("DECISION NEEDED: retry by hand"), "{text}");
        assert!(
            text.contains(
                "rk land feature --repo /checkouts/code-repo --target main --task TKT-1 --force --reason \
                 'human resolved attempts-exhausted'"
            ),
            "{text}"
        );
    }

    #[test]
    fn the_dispatch_key_names_the_whole_candidate() {
        let key = ctx().dispatch_key();
        for component in ["code-repo", "feature", "abc123", "main", "TKT-1"] {
            assert!(key.split('\0').any(|part| part == component), "{key:?}");
        }
    }
}
