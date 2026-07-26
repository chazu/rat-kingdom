// reviewer-drives-rework: a reviewer's verdict *drives* the next action.
// A rat implements the task; a reviewer inspects the branch and records an
// APPROVE / REWORK / STOP verdict as an artifact tuple. The workflow `read`s
// that verdict and `when`-routes on it, inside a bounded `repeat`:
//
//   APPROVE -> accept: leave the reviewed branch and finish (the run completes,
//              so the orchestrator merges the branch on dismissal)
//   REWORK  -> spawn a rework rat on the branch, then re-review (up to N rounds)
//   STOP    -> abort: `stop` fails the instance so the branch is discarded
//
// Rework chains onto the reviewed branch (each spawn bases its worktree on
// ctx.activeBranch), so the chain tip accumulates every round's commits — that
// tip is the approved branch. Like code-review and fix-then-verify, the run
// consolidates work onto a branch rather than merging to main itself; the
// difference here is that the reviewer's verdict, not a human, decides whether
// the run completes (accept) or fails (discard).
//
// Unlike code-review (which only parks the branch for a human), this loop runs
// review -> rework -> re-review to a clean verdict autonomously. The `repeat`
// cap is a hard bound: at most `maxRounds` review rounds ever run, so the
// "steps run once" safety property is preserved. If the reviewer never
// APPROVEs within maxRounds, the loop simply ends and the run completes on the
// last reworked branch.
//
//   rk workflow run reviewer-drives-rework --param taskId=fix-login \
//     --param description="Fix the login redirect loop" --param maxRounds=3
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "reviewer-drives-rework"
	description: "reviewer APPROVE/REWORK/STOP verdict routes merge / loop-back / abort"

	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		workTimeout: {type: "string", required: false, default: "30m"}
		reviewTimeout: {type: "string", required: false, default: "15m"}
		// Hard cap on review rounds (schema enforces 1..=100).
		maxRounds: {type: "int", required: false, default: 3}
	}

	agents: {
		// Implementation and rework get the strong model; review runs cheaper.
		default:  {harness: "claude"}
		reviewer: {harness: "claude", model: "haiku"}
	}

	steps: [
		// 1. The rat implements the task.
		{
			type: "spawn"
			role: "rat"
			task: {
				title:       _input.taskId
				description: _input.description
			}
		},
		{type: "wait", timeout: _input.workTimeout},
		{type: "evaluate", expect: {is_error: false}},
		// Keep the branch: the reviewer (and any rework) build on it.
		{type: "dismiss", noMerge: true},

		// 2. Review -> route -> maybe rework, up to maxRounds times.
		{
			type: "repeat"
			max:  _input.maxRounds
			steps: [
				{
					type:  "spawn"
					role:  "reviewer"
					agent: "reviewer"
					task: {
						title: "review-" + _input.taskId
						description: """
							Review the changes on your current branch ({{ctx.activeBranch}})
							against the task: \(_input.description)

							Compare with: git log main..HEAD and git diff main...HEAD

							Decide APPROVE (ready to merge), REWORK (fixable issues remain),
							or STOP (fundamentally wrong — do not merge). Record your verdict
							before finishing so the workflow can route on it:
							rk out artifact $RK_REPO review --payload '{"task": "\(_input.taskId)", "recommendation": "APPROVE|REWORK|STOP", "notes": "..."}'
							"""
					}
				},
				{type: "wait", timeout: _input.reviewTimeout},
				{type: "evaluate", expect: {is_error: false}},
				// Lift THIS round's review verdict into ctx.var.verdict.
				// `fromAgent` binds the read to the reviewer just spawned above
				// (TKT-161): (artifact, <repo>, review) is shared by every
				// reviewer in the repo, so an unbound "newest wins" read can
				// lift a concurrent instance's verdict — or, in this loop, the
				// PREVIOUS round's, if this round's reviewer recorded nothing.
				// Either way the routing below acts on a review of work it is
				// not looking at. Bound, a silent reviewer times the step out
				// and parks the branch instead of looping on a stale verdict.
				{
					type:      "read"
					category:  "artifact"
					identity:  "review"
					fromAgent: true
					field:     "recommendation"
					into:      "verdict"
					timeout:   "2m"
				},
				{
					type: "when"
					var:  "verdict"
					cases: {
						// Accept: clean up the reviewer's worktree (keeping the
						// reviewed branch) and leave the loop. The run completes,
						// so the orchestrator merges that branch on dismissal.
						"APPROVE": [
							{type: "dismiss", noMerge: true},
							{type: "break"},
						]
						// Abort: reviewer says the work is fundamentally wrong.
						"STOP": [
							{type: "dismiss", noMerge: true},
							{type: "stop", reason: "reviewer requested STOP for " + _input.taskId},
						]
						// Fix and re-review: a rework rat builds on the branch,
						// then the loop re-enters and the reviewer looks again.
						"REWORK": [
							{type: "dismiss", noMerge: true},
							{
								type: "spawn"
								role: "rat"
								task: {
									title: "rework-" + _input.taskId
									description: """
										You are on branch {{ctx.activeBranch}}. Address the reviewer's
										REWORK feedback for the task: \(_input.description)

										Read the latest review notes: rk scan artifact $RK_REPO
										Commit your changes; a fresh reviewer will re-examine the branch.
										"""
								}
							},
							{type: "wait", timeout: _input.workTimeout},
							{type: "evaluate", expect: {is_error: false}},
							{type: "dismiss", noMerge: true},
						]
					}
					// Unrecognized verdict: park unmerged and abort loudly.
					default: [
						{type: "dismiss", noMerge: true},
						{type: "stop", reason: "unrecognized review verdict for " + _input.taskId},
					]
				},
			]
		},
	]
}
