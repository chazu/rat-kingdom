// land-on-approve: a rat implements a change on a work branch, a (cheaper)
// reviewer chained off that branch records a verdict for a human to read, then
// the branch parks behind a HUMAN APPROVAL GATE. On approval the work is LANDED
// directly onto the target branch (main by default) — the workflow does the
// final merge itself. On rejection the branch is held unmerged for inspection
// and the run COMPLETES cleanly.
//
//   rk workflow run land-on-approve --param taskId=risky-change \
//     --param description="Rework the payment retry logic"
//
// This closes the last manual hop in autonomous review: a reviewer chained off
// a work branch used to be able to only COMPLETE the run and leave the branch
// for a human to merge, because its own dismiss merges into its BASE (the work
// branch), never main. The `land` step names {branch, target} explicitly, so an
// APPROVE lands the reviewed branch straight onto main.
//
// While parked, read the review and inspect the branch, then decide:
//   rk scan artifact                 # read the reviewer's verdict
//   rk workflow list                 # find the instance id (wf-...)
//   rk approve <instance>            # -> lands the work on `target`
//   rk reject  <instance>            # -> branch held unmerged, run completes
//
// SAFETY: `land` merges with no review of its own — it is reached ONLY through
// this approval gate's APPROVE branch. No decision before approvalTimeout fails
// closed (treated as not-approved -> held unmerged).
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "land-on-approve"
	description: "rat implements, reviewer verdicts, an approval gate lands the branch on main"
	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		// Where an approved branch lands. The whole point of `land` over a
		// plain `dismiss`: the reviewer's base is the work branch, not this.
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
		// Keep the work branch: the reviewer chains off it, and the human lands
		// it later. This tears down the rat's worktree but preserves the branch.
		{type: "dismiss", noMerge: true},
		// The reviewer spawns from the rat's branch automatically (the runner
		// bases new worktrees on ctx.activeBranch) and records a verdict a human
		// reads before approving. Its own base is the work branch, so it could
		// never land the work on main by itself — that is what `land` fixes.
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
		// branch/worktree survive for the merge decision.
		{type: "gate", gateType: "approval", timeout: _input.approvalTimeout},
		// Lift the human's verdict into ctx.var.approved. The gate leaves a
		// workflow_approval event behind (both for a real decision and the
		// fail-closed timeout), so this read resolves immediately.
		{
			type:     "read"
			category: "event"
			identity: "workflow_approval"
			field:    "approved"
			into:     "approved"
			timeout:  "5m"
		},
		{
			type: "when"
			var:  "approved"
			cases: {
				// Approved: tear down the reviewer's worktree first (so its
				// branch is no longer checked out), then LAND that branch —
				// which carries the reviewed work — onto the target. `land`
				// deletes the merged branch. The run completes.
				"true": [
					{type: "dismiss", noMerge: true},
					{type: "land", branch: "{{ctx.activeBranch}}", target: _input.target},
				]
				// Rejected (or fail-closed timeout): tear down the worktree but
				// PRESERVE the branch, unmerged, for a human. The run still
				// completes — a veto is a normal outcome, not a failure.
				"false": [
					{type: "dismiss", noMerge: true},
				]
			}
			// An unrecognized decision value is a bug, not a veto: hold the
			// branch unmerged and abort loudly rather than silently landing.
			default: [
				{type: "dismiss", noMerge: true},
				{type: "stop", reason: "unrecognized approval decision for " + _input.taskId},
			]
		},
	]
}
