// Rat Kingdom repository policy. This file is versioned as `.rk/repo.cue`,
// validated during onboarding, and copied into the operator-owned repository
// registry only after its exact content digest is activated.
package repository

repo: #RepositoryPolicy

#RepositoryPolicy: {
	work: #WorkPolicy | *{}
	delivery: #DeliveryPolicy | *{}
	landing: #LandingPolicy | *{}
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
}
