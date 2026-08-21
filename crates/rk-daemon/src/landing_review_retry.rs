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

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::time::Duration;

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

/// Activated repository bounds for the delay before a review-death
/// replacement is dispatched — a SEPARATE knob from [`ReviewDeathPolicy`]'s
/// count/spend ceilings (module doc: policy is the whole ladder here, and
/// this is one more rung of it, not a replacement for the others). Kept as
/// its own type so `route`'s count/spend decision stays exactly what it was
/// before this policy existed — this is consulted only once `route` has
/// already decided to [`ReviewDeathRoute::Dispatch`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ReviewDeathBackoffPolicy {
    /// Delay before the first replacement (`attempt == 1`). Zero preserves
    /// the pre-backoff immediate-dispatch behavior exactly — see
    /// [`retry_delay`].
    pub(crate) base_delay: Duration,
    /// Percent scaling applied per additional attempt beyond the first —
    /// `100` holds the delay flat, `200` doubles it each attempt (integer,
    /// not float, so this policy can keep deriving `Eq` like its siblings).
    pub(crate) backoff_pct: u32,
    /// Hard ceiling the computed delay (jitter included) never exceeds.
    pub(crate) max_delay: Duration,
    /// Percent of the clamped backoff added as jitter, uniform over
    /// `[0, jitter_pct]`. `0` disables jitter.
    pub(crate) jitter_pct: u32,
}

impl ReviewDeathBackoffPolicy {
    pub(crate) fn from_landing(policy: &rk_workflow::LandingPolicy) -> Self {
        let defaults = rk_workflow::LandingPolicy::default();
        let parse = |s: &str, fallback: &str| -> Duration {
            crate::workflow_exec::parse_duration(s)
                .or_else(|_| crate::workflow_exec::parse_duration(fallback))
                .unwrap_or_default()
        };
        Self {
            base_delay: parse(
                &policy.review_death_retry_delay,
                &defaults.review_death_retry_delay,
            ),
            backoff_pct: policy.review_death_retry_backoff_pct,
            max_delay: parse(
                &policy.review_death_retry_max_delay,
                &defaults.review_death_retry_max_delay,
            ),
            jitter_pct: policy.review_death_retry_jitter_pct,
        }
    }
}

impl Default for ReviewDeathBackoffPolicy {
    fn default() -> Self {
        Self::from_landing(&rk_workflow::LandingPolicy::default())
    }
}

/// Delay before dispatching `attempt` (1-based), before jitter: `base_delay`
/// scaled by `backoff_pct / 100` for every attempt past the first, clamped to
/// `max_delay`. Zero `base_delay` short-circuits to zero regardless of
/// `backoff_pct`/`max_delay` — an explicit zero-delay policy stays exactly
/// immediate, never inflated by a nonzero clamp or percent.
///
/// Clamps against `max_delay` in `f64` seconds BEFORE converting back to a
/// `Duration`: a schema-valid but extreme `backoff_pct` (up to `u32::MAX`)
/// compounded across up to 32 growth steps, or a schema-valid but extreme
/// `base_delay`, can multiply out to a second count `Duration::mul_f64`
/// cannot represent — that call panics rather than saturating. Computing the
/// clamp in `f64` first and only ever constructing a `Duration` from a value
/// already bounded by (representable) `max_delay` keeps this infallible.
fn backoff_before_jitter(policy: &ReviewDeathBackoffPolicy, attempt: u32) -> Duration {
    if policy.base_delay.is_zero() {
        return Duration::ZERO;
    }
    let growth = attempt.saturating_sub(1).min(32);
    let factor = (f64::from(policy.backoff_pct) / 100.0).powi(growth as i32);
    let scaled_secs = policy.base_delay.as_secs_f64() * factor.max(0.0);
    clamp_secs_to_duration(scaled_secs, policy.max_delay)
}

/// The full retry delay for `attempt`: [`backoff_before_jitter`] plus
/// `jitter_pct` percent of jitter, drawn from the caller-supplied
/// `jitter_unit` — a value in `[0.0, 1.0]` — so this stays pure and
/// deterministic: no seam for real randomness lives here, the caller draws
/// it from `crate::landing::RetrySchedule` (in production `rand`, in a test
/// a fixed value). The final result is re-clamped to `max_delay`: at the ceiling,
/// jitter has no further room to add wait, which is what "hard ceiling"
/// means.
pub(crate) fn retry_delay(
    policy: &ReviewDeathBackoffPolicy,
    attempt: u32,
    jitter_unit: f64,
) -> Duration {
    let base = backoff_before_jitter(policy, attempt);
    if base.is_zero() || policy.jitter_pct == 0 {
        return base;
    }
    let jitter_unit = jitter_unit.clamp(0.0, 1.0);
    let jitter_frac = (f64::from(policy.jitter_pct) / 100.0) * jitter_unit;
    let scaled_secs = base.as_secs_f64() * (1.0 + jitter_frac);
    clamp_secs_to_duration(scaled_secs, policy.max_delay)
}

/// Bounds a raw second count (which may be non-finite or larger than
/// `Duration` can represent, e.g. from compounding an extreme schema-valid
/// policy value) to `[0, max_delay]` and only then converts to a `Duration`,
/// so the conversion itself can never panic the way `Duration::mul_f64`/
/// `from_secs_f64` do on an out-of-range result.
fn clamp_secs_to_duration(secs: f64, max_delay: Duration) -> Duration {
    if !secs.is_finite() || secs <= 0.0 {
        return Duration::ZERO;
    }
    let bounded = secs.min(max_delay.as_secs_f64());
    Duration::try_from_secs_f64(bounded).unwrap_or(max_delay)
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

    /// Durable marker payload; top-level branch/head fields support exact
    /// probes. `not_before` is the durable backoff schedule chosen when this
    /// attempt was first decided (module doc on [`ReviewDeathBackoffPolicy`])
    /// — `None` for a withhold marker (no dispatch to schedule) or a marker
    /// seeded before this policy existed, either of which must read back as
    /// "no wait", not as an error.
    pub(crate) fn marker_payload(
        &self,
        attempt: u32,
        instance_id: &str,
        state: &str,
        not_before: Option<DateTime<Utc>>,
    ) -> Value {
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
            "not_before": not_before.map(|dt| dt.to_rfc3339()),
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

    fn backoff_policy() -> ReviewDeathBackoffPolicy {
        ReviewDeathBackoffPolicy {
            base_delay: Duration::from_secs(30),
            backoff_pct: 200,
            max_delay: Duration::from_secs(600),
            jitter_pct: 20,
        }
    }

    #[test]
    fn the_first_retry_delay_is_the_base_delay_unjittered() {
        assert_eq!(
            retry_delay(&backoff_policy(), 1, 0.0),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn the_backoff_progression_is_bounded_exponential() {
        let policy = backoff_policy();
        // 200% per attempt: 30s, 60s, 120s, 240s, 480s, then clamped at 600s.
        assert_eq!(retry_delay(&policy, 1, 0.0), Duration::from_secs(30));
        assert_eq!(retry_delay(&policy, 2, 0.0), Duration::from_secs(60));
        assert_eq!(retry_delay(&policy, 3, 0.0), Duration::from_secs(120));
        assert_eq!(retry_delay(&policy, 4, 0.0), Duration::from_secs(240));
        assert_eq!(retry_delay(&policy, 5, 0.0), Duration::from_secs(480));
        assert_eq!(retry_delay(&policy, 6, 0.0), Duration::from_secs(600));
        assert_eq!(retry_delay(&policy, 20, 0.0), Duration::from_secs(600));
    }

    #[test]
    fn jitter_stays_within_the_configured_percent_and_scales_with_the_unit() {
        let policy = backoff_policy();
        // jitter_pct 20 over a 30s base: [30s, 36s].
        assert_eq!(retry_delay(&policy, 1, 0.0), Duration::from_secs(30));
        assert_eq!(retry_delay(&policy, 1, 1.0), Duration::from_secs(36));
        assert_eq!(retry_delay(&policy, 1, 0.5), Duration::from_millis(33_000));
        // A jitter_unit outside [0, 1] is clamped rather than trusted.
        assert_eq!(retry_delay(&policy, 1, 2.0), Duration::from_secs(36));
        assert_eq!(retry_delay(&policy, 1, -1.0), Duration::from_secs(30));
    }

    #[test]
    fn max_delay_is_a_hard_clamp_even_with_jitter_at_the_ceiling() {
        let policy = backoff_policy();
        // Attempt 6 is already clamped to max_delay before jitter; jitter
        // must not be able to push it past that ceiling.
        assert_eq!(retry_delay(&policy, 6, 1.0), policy.max_delay);
        assert_eq!(retry_delay(&policy, 50, 1.0), policy.max_delay);
    }

    #[test]
    fn extreme_schema_valid_backoff_pct_and_attempt_stay_bounded_without_panicking() {
        // backoff_pct at u32::MAX compounded over the growth cap, and an
        // attempt ordinal far past it, used to overflow `Duration::mul_f64`
        // before it ever reached the advertised clamp — this must not panic
        // and must still land exactly on max_delay.
        let policy = ReviewDeathBackoffPolicy {
            base_delay: Duration::from_secs(u64::MAX / 4),
            backoff_pct: u32::MAX,
            max_delay: Duration::from_secs(600),
            jitter_pct: 20,
        };
        for attempt in [1, 2, 32, 1000, u32::MAX] {
            assert_eq!(retry_delay(&policy, attempt, 1.0), policy.max_delay);
        }
    }

    #[test]
    fn extreme_schema_valid_jitter_pct_stays_bounded_without_panicking() {
        // jitter_pct at u32::MAX scales the post-jitter multiplier into the
        // billions — `base.mul_f64` used to overflow before the re-clamp.
        let policy = ReviewDeathBackoffPolicy {
            base_delay: Duration::from_secs(30),
            backoff_pct: 100,
            max_delay: Duration::from_secs(600),
            jitter_pct: u32::MAX,
        };
        assert_eq!(retry_delay(&policy, 1, 1.0), policy.max_delay);
    }

    #[test]
    fn zero_base_delay_stays_immediate_regardless_of_backoff_or_jitter() {
        let policy = ReviewDeathBackoffPolicy {
            base_delay: Duration::ZERO,
            backoff_pct: 200,
            max_delay: Duration::from_secs(600),
            jitter_pct: 50,
        };
        for attempt in [1, 2, 5] {
            assert_eq!(retry_delay(&policy, attempt, 1.0), Duration::ZERO);
        }
    }

    #[test]
    fn zero_jitter_pct_never_adds_wait() {
        let policy = ReviewDeathBackoffPolicy {
            jitter_pct: 0,
            ..backoff_policy()
        };
        assert_eq!(retry_delay(&policy, 1, 1.0), Duration::from_secs(30));
    }

    #[test]
    fn from_landing_reads_the_repository_policy_knobs() {
        let landing = rk_workflow::LandingPolicy {
            review_death_retry_delay: "45s".into(),
            review_death_retry_backoff_pct: 150,
            review_death_retry_max_delay: "5m".into(),
            review_death_retry_jitter_pct: 10,
            ..rk_workflow::LandingPolicy::default()
        };
        let policy = ReviewDeathBackoffPolicy::from_landing(&landing);
        assert_eq!(policy.base_delay, Duration::from_secs(45));
        assert_eq!(policy.backoff_pct, 150);
        assert_eq!(policy.max_delay, Duration::from_secs(300));
        assert_eq!(policy.jitter_pct, 10);
    }

    #[test]
    fn from_landing_falls_back_to_the_default_duration_on_a_malformed_policy_string() {
        let landing = rk_workflow::LandingPolicy {
            review_death_retry_delay: "not-a-duration".into(),
            ..rk_workflow::LandingPolicy::default()
        };
        let policy = ReviewDeathBackoffPolicy::from_landing(&landing);
        let defaults = ReviewDeathBackoffPolicy::default();
        assert_eq!(policy.base_delay, defaults.base_delay);
    }
}
