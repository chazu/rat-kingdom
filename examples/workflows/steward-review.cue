// steward-review: the review-only remainder of the steward after the
// daemon-native landing pipeline (Phase 3, TKT-01M036PSEHTMD3S5D2JFAG7XVY)
// took over gates, the commit-keyed verdict-cache probe, and
// APPROVE/REWORK/STOP routing. See
// docs/proposals/daemon-native-landing-pipeline.md §2.3-2.5.
//
// This workflow's ONLY job is the one thing genuinely needing an LLM: spawn
// a reviewer chained onto a landing candidate's branch, wait for it, and
// confirm it finished cleanly. The reviewer records its own verdict as an
// artifact (`rk out artifact <repo> review --payload {...}`); the daemon-
// native `LandingPipeline` (crates/rk-daemon/src/landing.rs) parks on that
// tuple directly (`Space::rd`, `Pattern::for_commit`), not on this workflow
// instance's own completion state — a park-and-resume that survives a
// daemon restart even though this instance's own bookkeeping does not
// (design doc §2.6).
//
// Invoked PROGRAMMATICALLY by `LandingPipeline::request_review` on a
// verdict-cache miss — never reactor-fired, and never run by hand. Exactly
// the first three steps of the old mega-workflow's `_reviewArm`
// (`examples/workflows/steward.cue:343-371`), nothing else: no gates, no
// verdict read, no routing — all three now live in the daemon-native
// pipeline instead of CUE.
//
// CUTOVER STATUS (Phase 4, TKT-01M036PSF2WV7NHZE00G2EFCVK): the operator
// executed the §6 runbook on 2026-08-16 — the live
// `steward-landing-on-completion` trigger (action: land) replaced
// `steward-on-completion` in the global triggers directory, so THIS
// workflow (spawned programmatically by `LandingPipeline::request_review`)
// is now the one genuinely live review path. `examples/workflows/steward.cue`
// is no longer fired by anything; it remains in the tree only as the
// pre-cutover reference implementation, plus its dedicated schema/routing
// tests in `crates/rk-workflow/tests/examples.rs` and
// `crates/rk-daemon/tests/workflow_verdict_cache.rs`. Deleting it and that
// test coverage together (~450 lines) is tracked separately:
// TKT-01M048ASYM00N37EBK1VM7FH5H, "Remove obsolete steward CUE gate
// definitions after pipeline cutover".
//
// Install with `rk workflow install examples/workflows/steward-review.cue`
// (global) or into `<repo>/.rk/workflows/` (repo-local) — same install/drift
// story as every other shipped workflow.
package workflow

workflow: {
	name:        "steward-review"
	description: "review-only: spawn a reviewer chained onto a landing candidate's branch and wait for its verdict"

	params: {
		// Identifies the candidate in the reviewer's task text. Templated by
		// LandingPipeline from the queued candidate's `task`.
		taskId: {type: "string", required: false, default: "unknown"}
		// The landing candidate's branch — the reviewer chains onto it
		// (spawn.branch is the new agent's base). Required: LandingPipeline
		// never requests a review without a real branch to chain onto.
		branch: {type: "string", required: true}
		// The repo the candidate belongs to — scopes the reviewer's verdict
		// artifact so LandingPipeline's `Pattern::for_commit` probe (scoped to
		// the same repo) finds it.
		repo: {type: "string", required: false, default: "rat-kingdom"}
		// Where the candidate would land on APPROVE. Not used by any step in
		// THIS workflow (routing is now daemon-native) — threaded through only
		// so the reviewer's task description can name it for `git diff`.
		target: {type: "string", required: false, default: "main"}
		// The candidate's branch-tip SHA. Threaded into the reviewer's task
		// and its verdict artifact so LandingPipeline's commit-keyed cache
		// probe (Phase 2) can key on it.
		headSha: {type: "string", required: false, default: ""}
		// Same invariant as the old mega-workflow's `reviewTimeout`
		// (`examples/workflows/steward.cue`): must stay comfortably ABOVE the
		// daemon's `supervisor.stuck_after_secs`, or the wait here hard-fails
		// before the sweep's soft steer gets a chance to nudge a slow reviewer
		// back to a clean `rk done`.
		reviewTimeout: {type: "string", required: false, default: "15m"}
	}

	agents: {
		default:  {harness: "codex", model: "gpt-5.6-luna"}
		reviewer: {harness: "codex", model: "gpt-5.6-luna"}
	}

	steps: [
		// A cheap reviewer chains onto the candidate branch (spawn.branch is
		// the base), so its worktree carries the completed work and `git
		// diff`/`git log` in its task description see it.
		{
			type:   "spawn"
			role:   "reviewer"
			agent:  "reviewer"
			branch: _input.branch
			task: {
				title: "steward-review-" + _input.taskId
				description: """
					You are the steward's reviewer on branch {{ctx.activeBranch}}
					(head \(_input.headSha)), chained off a rat's completed work for:
					\(_input.taskId)

					Compare with: git log \(_input.target)..HEAD and git diff \(_input.target)...HEAD

					Decide APPROVE (clean, safe to auto-merge), REWORK (fixable issues
					remain), or STOP (fundamentally wrong / needs a human call). Record
					the verdict before finishing so the daemon-native landing pipeline
					can route on it:
					rk out artifact \(_input.repo) review --payload '{"task": "\(_input.taskId)", "recommendation": "APPROVE|REWORK|STOP", "notes": "...", "head_sha": "\(_input.headSha)", "branch": "\(_input.branch)"}'
					"""
			}
		},
		{type: "wait", timeout: _input.reviewTimeout},
		// The reviewer session must itself have finished cleanly. Whether it
		// actually recorded a verdict is not checked here — LandingPipeline's
		// own `space.rd` wait (bound to this exact branch/headSha) is what
		// decides APPROVE/REWORK/STOP/timeout; this instance's job ends here.
		{type: "evaluate", expect: {is_error: false}},
	]
}
