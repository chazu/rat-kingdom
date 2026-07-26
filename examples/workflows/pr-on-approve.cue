// pr-on-approve: a rat implements a change on a work branch, a (cheaper)
// reviewer chained off that branch records a verdict for a human to read, then
// the branch parks behind a HUMAN APPROVAL GATE. On approval the work is handed
// off as a PULL REQUEST against the target branch (main by default) — the
// workflow pushes the branch and opens a PR rather than merging it itself. On
// rejection the branch is held (never pushed) for inspection and the run
// COMPLETES cleanly.
//
//   rk workflow run pr-on-approve --param taskId=risky-change \
//     --param description="Rework the payment retry logic"
//
// This is the PR-mode sibling of land-on-approve.cue. Where `land-on-approve`
// merges the reviewed branch straight onto main via the `land` step, this
// workflow ends the APPROVE branch with `open_pr`, which ALWAYS opens a pull /
// merge request regardless of the repo's registered merge mode. So a repo whose
// default policy is Direct-merge can still opt a specific run into review-by-PR:
// the choice lives in the workflow, not the repo. The branch is pushed and left
// standing; a human (or a downstream CI/steward) does the final merge on the
// forge.
//
// While parked, read the review and inspect the branch, then decide:
//   rk scan artifact                 # read the reviewer's verdict
//   rk workflow list                 # find the instance id (wf-...)
//   rk approve <instance>            # -> pushes the branch + opens a PR
//   rk reject  <instance>            # -> branch held (not pushed), run completes
//
// SAFETY: `open_pr` opens a PR with no review of its own — it is reached ONLY
// through this approval gate's APPROVE branch. No decision before
// approvalTimeout fails closed (treated as not-approved -> branch held, no PR).
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "pr-on-approve"
	description: "rat implements, reviewer verdicts, an approval gate opens a PR against main"
	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		// Which branch the opened PR targets. The reviewer's base is the work
		// branch, not this — `open_pr` names the target explicitly.
		target: {type: "string", required: false, default: "main"}
		implTimeout: {type: "string", required: false, default: "30m"}
		reviewTimeout: {type: "string", required: false, default: "15m"}
		// How long to hold the branch waiting for a human decision. No response
		// by then fails closed (treated as not-approved).
		approvalTimeout: {type: "string", required: false, default: "24h"}
	}
	agents: {
		// Implementation gets the strong model, review runs cheaper.
		default: {harness: "claude"}
		reviewer: {harness: "claude", model: "haiku"}
	}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {title: _input.taskId, description: _input.description}
		},
		{type: "wait", timeout: _input.implTimeout},
		{type: "evaluate", expect: {is_error: false}}, // implementation must succeed
		// Keep the work branch: the reviewer chains off it, and the human opens
		// the PR later. This tears down the rat's worktree but preserves the
		// branch.
		{type: "dismiss", noMerge: true},
		// The reviewer spawns from the rat's branch automatically (the runner
		// bases new worktrees on ctx.activeBranch) and records a verdict a human
		// reads before approving. Its own base is the work branch, so it could
		// never open a PR against main by itself — that is what `open_pr` fixes.
		{
			type:  "spawn"
			role:  "reviewer"
			agent: "reviewer"
			task: {
				title: "review-" + _input.taskId
				description: """
					Review the changes on your current branch ({{ctx.activeBranch}})
					against the task: \(_input.description)

					Compare with: git log \(_input.target)..HEAD and git diff \(_input.target)...HEAD

					Record your verdict before finishing (a human reads it before approving):
					rk out artifact $RK_REPO review --payload '{"task": "\(_input.taskId)", "recommendation": "APPROVE|REWORK|STOP", "notes": "..."}'
					"""
			}
		},
		{type: "wait", timeout: _input.reviewTimeout},
		{type: "evaluate", expect: {is_error: false}},
		// Park behind the human. Blocks until `rk approve`/`rk reject` for this
		// instance, or approvalTimeout elapses (then a fail-closed
		// {approved: false}). The reviewer is NOT dismissed here, so its
		// branch/worktree survive for the PR decision.
		{type: "gate", gateType: "approval", timeout: _input.approvalTimeout},
		// Lift the human's verdict into ctx.var.approved. The gate leaves a
		// workflow_approval event behind (both for a real decision and the
		// fail-closed timeout), so this read resolves immediately.
		//
		// `fromInstance` binds it to THIS run's decision (TKT-172). Unbound,
		// (event, <repo>, workflow_approval) is shared by every gated instance
		// on the repo and the newest decision wins whoever it was meant for —
		// so a peer's approval could open a PR for THIS branch below.
		{
			type:         "read"
			category:     "event"
			identity:     "workflow_approval"
			fromInstance: true
			field:        "approved"
			into:         "approved"
			timeout:      "5m"
		},
		{
			type: "when"
			var:  "approved"
			cases: {
				// Approved: tear down the reviewer's worktree first (so its
				// branch is no longer checked out), then OPEN A PR for that
				// branch — which carries the reviewed work — against the target.
				// `open_pr` pushes the branch and leaves it standing; the run
				// completes with the PR awaiting a human merge on the forge.
				"true": [
					{type: "dismiss", noMerge: true},
					{type: "open_pr", branch: "{{ctx.activeBranch}}", target: _input.target},
					// GATE THE PR RESULT. A push/auth failure is a clean
					// {pr_opened: false} (not an error); fail closed so an
					// approved-but-not-pushed branch surfaces in rk inbox instead
					// of the run completing as if the PR opened.
					{type: "evaluate", expect: {pr_opened: true}},
				]
				// Rejected (or fail-closed timeout): tear down the worktree but
				// PRESERVE the branch, unpushed, for a human. The run still
				// completes — a veto is a normal outcome, not a failure.
				"false": [
					{type: "dismiss", noMerge: true},
				]
			}
			// An unrecognized decision value is a bug, not a veto: hold the
			// branch and abort loudly rather than silently opening a PR.
			default: [
				{type: "dismiss", noMerge: true},
				{type: "stop", reason: "unrecognized approval decision for " + _input.taskId},
			]
		},
	]
}
