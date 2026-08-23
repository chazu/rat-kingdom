// Rat Kingdom repository policy. This file is versioned as `.rk/repo.cue`,
// validated during onboarding, and copied into the operator-owned repository
// registry only after its exact content digest is activated.
package repository

repo: #RepositoryPolicy

#RepositoryPolicy: {
	work: #WorkPolicy | *{}
	delivery: #DeliveryPolicy | *{}
	landing: #LandingPolicy | *{}
	reap: #ReapPolicy | *{}
	phaseLatency: #PhaseLatencyPolicy | *{}
}

#WorkPolicy: {
	// Supported placeholders: {{agent}}, {{task}}, {{repo}}, and {{role}}.
	// Both templates must include {{agent}} so concurrent workers cannot collide.
	branch: string | *"rat/{{agent}}/{{task}}"
	worktree: string | *"{{repo}}/{{agent}}"
}

#DeliveryPolicy: {
	// `agent-base` carries the completed worker's actual base through steward;
	// any other value is a fixed branch name such as `main` or `develop`.
	target: string | *"agent-base"
	mode: "merge" | "merge-push" | "push-branch" | "pr" | *"merge"
	remote: string | *"origin"
	// Supported placeholders: {{branch}}, {{target}}, and {{repo}}.
	remoteBranch: string | *"{{branch}}"
	deleteSource: bool | *true
}

// Landing-pipeline gate policy (Phase 4 of the steward remediation): the
// same protectedPaths/maxDiffFiles/maxDiffLines/gateTimeout/reviewTimeout
// knobs `examples/workflows/steward.cue`'s mega-workflow used to expose as
// workflow params, now owned by the daemon-native LandingPipeline
// (crates/rk-daemon/src/landing.rs) instead of CUE.
#LandingPolicy: {
	// POLICY GUARDRAIL (#19): an ERE matched against changed file paths, run
	// through the repo's `steward-protected-paths` named check.
	protectedPaths: string | *"(^|/)(\\.github|\\.rk|migrations)/"
	// DIFF-SCOPE GUARDRAIL (#20): 0 disables the budget. Run through the
	// repo's `steward-diff-scope` named check.
	maxDiffFiles: int | *50
	maxDiffLines: int | *2000
	// Wall-clock bound for the repo's real `verify` check.
	gateTimeout: string | *"60m"
	// Wall-clock bound the landing pipeline waits on a review verdict before
	// checking whether the reviewer is still alive (liveness-aware wait).
	reviewTimeout: string | *"15m"
	// Hard ceiling on the review wait: a reviewer still alive past
	// reviewTimeout is not abandoned until this ceiling. Only a reviewer that
	// goes terminal without a verdict escalates before it.
	reviewMaxWait: string | *"45m"
	// UNATTENDED REWORK (crates/rk-daemon/src/landing_rework.rs): whether a
	// reviewer REWORK classified as delegated-LLM work may dispatch a rework
	// agent from the reviewed branch at its exact head, with no human polling.
	// A verdict the classifier cannot positively read as a bounded correction
	// is held for a human regardless of this switch.
	reworkAutoDispatch: bool | *true
	// Hard ceiling on rework agents per reviewed branch. 0 disables dispatch.
	maxReworkAttempts: int | *1
	// Hard ceiling on cumulative USD across one review/rework chain. 0 = unlimited.
	reworkMaxUsd: int | *25
	// UNATTENDED CONFLICT RECOVERY (crates/rk-daemon/src/landing_conflict.rs):
	// whether a landing-time merge CONFLICT (the candidate never even built,
	// so there is no reviewer verdict) may dispatch a bounded correction agent
	// from the held source branch at its exact head, with no human polling. A
	// protected-path hit, an over-budget diff, or unreadable conflict evidence
	// is held for a human regardless of this switch.
	conflictReworkAutoDispatch: bool | *true
	// Hard ceiling on correction agents per conflicted branch. 0 disables dispatch.
	maxConflictReworkAttempts: int | *1
	// Hard ceiling on cumulative USD across one conflict/correction chain. 0 = unlimited.
	conflictReworkMaxUsd: int | *25
	// SHADOW REVIEW (phase-2 P4a): when non-empty, every review request ALSO
	// spawns a second, non-blocking reviewer on this model, chained onto the
	// same candidate. Its verdict is recorded for comparison against the
	// primary reviewer's (a `review-shadow-comparison` artifact) but never
	// gates landing and never replaces the primary reviewer — this workflow's
	// own `agents.reviewer` stays the one and only verdict LandingPipeline
	// routes on. Empty (the default) disables shadow review entirely — the
	// acceptance bar is default-unchanged until an explicit follow-up ticket
	// flips it; a repo opts in explicitly.
	shadowReviewModel: string | *""
	// Harness for the shadow reviewer. Ignored when shadowReviewModel is empty.
	shadowReviewHarness: string | *""
	// REVIEW-DEATH RETRY (crates/rk-daemon/src/landing_review_retry.rs): whether
	// a reviewer that goes terminal without ever producing a verdict may be
	// retried unattended with a fresh reviewer against the same exact head,
	// with no human polling.
	reviewDeathAutoRetry: bool | *true
	// Hard ceiling on replacement reviewers per dead review. 0 disables retry.
	maxReviewDeathAttempts: int | *1
	// Hard ceiling on cumulative USD across one review-death retry chain. 0 = unlimited.
	reviewDeathMaxUsd: int | *10
	// Delay before the FIRST review-death replacement is dispatched. Bounded
	// backoff is on by default so an unconfigured repo does not re-dispatch
	// into the same infrastructure blip on the same tick; "0s" is the explicit
	// opt-out that restores pre-backoff immediate dispatch exactly.
	reviewDeathRetryDelay: string | *"30s"
	// Percent scaling applied to the delay per additional replacement beyond
	// the first — 100 holds it flat, 200 doubles it each attempt.
	reviewDeathRetryBackoffPct: int | *200
	// Hard ceiling the computed delay (jitter included) never exceeds.
	reviewDeathRetryMaxDelay: string | *"10m"
	// Percent of the clamped backoff added as jitter, uniform over
	// [0, jitterPct]. 0 disables jitter.
	reviewDeathRetryJitterPct: int | *20
	// PROTECTED FINAL TARGETS: target branches this repo treats as
	// protected/final delivery destinations. A landing edge whose target is
	// one of these runs the repo's full named check exactly once, through
	// the daemon's managed-verification proof-key cache. Any other target is
	// an inner child-to-parent edge and runs only the checks focusedChecks
	// below selects, never the full check by default.
	protectedTargets: [...string] | *["main"]
	// FOCUSED CHECKS: ordered rules mapping changed-path patterns to the
	// named checks (.rk/checks.cue) an inner landing edge runs INSTEAD OF the
	// full check. Every rule whose paths matches at least one changed file —
	// or that declares no paths at all, an unconditional catch-all —
	// contributes its checks, deduped in first-seen order. No rule matching
	// means no additional check runs beyond protectedPaths/diffScope: an
	// inner edge never falls back to the full suite by default.
	focusedChecks: [...#FocusedCheckRule] | *[]
}

#FocusedCheckRule: {
	// POSIX ERE alternatives matched against each changed path (the same
	// engine protectedPaths uses — grep -E). Empty matches unconditionally.
	paths: [...string] | *[]
	// Free-form label surfaced in landing events as this rule's selection
	// reason — a "named check class" (e.g. "docs", "rust-fast").
	class: string | *""
	// Named checks (.rk/checks.cue) this rule contributes when it matches.
	checks: [...string]
}

// PHASE LATENCY POLICY (TKT-01M0P974MQK5XE1MR9KQCWT654): per-phase warning
// and intervention wall-clock targets over the durable task-to-main
// phase-span substrate (crates/rk-daemon/src/span.rs). STACK NEUTRALITY: the
// daemon has no built-in notion of what latency is normal for any phase, so
// this defaults to an empty map (no targets, nothing ever breaches) — a repo
// opts in per phase by name, matching Phase::as_str() in span.rs (e.g.
// "verification", "landing_prep", "semantic_review").
#PhaseLatencyPolicy: {
	targets: [string]: #PhaseLatencyTarget
}

#PhaseLatencyTarget: {
	// Below this elapsed wall-clock time, the phase is healthy: no attention
	// item. Empty disables the warning tier for this phase.
	warning: string | *""
	// Above this elapsed wall-clock time, the phase escalates from warning to
	// intervention. Empty disables the intervention tier for this phase.
	intervention: string | *""
}

// Regenerable build-artifact paths (relative to a worktree root) the daemon's
// worktree sweep reclaims from every terminal agent's worktree, any merge
// state. STACK NEUTRALITY: the daemon has no built-in notion of what any
// language's build directory is called, so this defaults to empty (reap
// nothing) — a repo that wants this reap names its own paths, e.g.
// ["target"] for a cargo workspace or ["node_modules", "dist"] for an npm
// one.
#ReapPolicy: {
	artifactPaths: [...string] | *[]
}
