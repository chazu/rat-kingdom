// code-review: a rat implements the task, then a (cheaper) reviewer examines
// the rat's branch and records a recommendation. Work is left on the branch
// for a human to merge after reading the review — nothing auto-merges.
//
//   rk workflow run code-review --param taskId=add-caching \
//     --param description="Add response caching to the API layer"
//
// The reviewer spawns from the rat's branch automatically (the runner bases
// new worktrees on ctx.activeBranch). Review lands as an artifact tuple:
//   rk scan artifact
workflow: {
	name:        "code-review"
	description: "rat implements, reviewer validates, human merges"

	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		reviewTimeout: {type: "string", required: false, default: "15m"}
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
			task: {
				title:       _input.taskId
				description: _input.description
			}
		},
		{type: "wait", timeout: "30m"},
		{type: "evaluate", expect: {is_error: false}},
		// Keep the branch: the reviewer needs it, and the human merges later.
		{type: "dismiss", noMerge: true},
		{
			type:  "spawn"
			role:  "reviewer"
			agent: "reviewer"
			task: {
				title: "review-" + _input.taskId
				description: """
					Review the uncommitted-to-main changes on your current branch
					({{ctx.activeBranch}}) against the task: \(_input.description)

					Compare with: git log main..HEAD and git diff main...HEAD

					Record your verdict before finishing:
					rk out artifact $RK_REPO review --payload '{"task": "\(_input.taskId)", "recommendation": "APPROVE|REWORK|STOP", "notes": "..."}'
					"""
			}
		},
		{type: "wait", timeout: _input.reviewTimeout},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},
	]
}
