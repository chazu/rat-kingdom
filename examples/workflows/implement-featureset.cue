// implement-featureset: a foreman owns the feature branch and delegates its
// child tickets to workers. Workers merge into the foreman's branch only after
// the foreman receives their completion message and accepts their changes.
// The final dismiss merges the integrated foreman branch into the workflow's
// target branch.
//
// Usage:
//   rk workflow run implement-featureset --param taskId=TKT-... \
//     --param taskDescription="Implement the feature described by the ticket"
//
// The taskId should normally be an epic/parent ticket whose children are the
// independent implementation slices. If it has no children, the foreman treats
// the parent itself as one worker task.
workflow: {
	name:        "implement-featureset"
	description: "foreman delegates child tickets, integrates them, and lands the feature branch"

	params: {
		taskId:          {type: "string", required: true}
		taskDescription: {type: "string", required: true}
		maxWorkers:      {type: "string", required: false, default: "5"}
		reviewMode:      {type: "string", required: false, default: "full"}
		check:            {type: "string", required: false, default: "cargo test --quiet"}
		timeout:          {type: "string", required: false, default: "120m"}
		budgetUsd:        {type: "int", required: false, default: 20}
	}

	// The cap applies to the foreman and every worker it dispatches. The daemon
	// inherits the instance cap when a foreman creates a child through `rk spawn`.
	budget: {max_usd: _input.budgetUsd}
	agents: {default: {harness: "claude"}}

	steps: [
		{
			type: "spawn"
			role: "foreman"
			coordination: {
				reports_to: "coordinator"
				descendant_policy: "rollup"
			}
			task: {
				title: "foreman-" + _input.taskId
				description: """
					FEATURE-SET ORCHESTRATION

					Parent ticket: \(_input.taskId)
					Feature description: \(_input.taskDescription)
					Maximum concurrent workers: \(_input.maxWorkers)
					Review mode: \(_input.reviewMode)
					Integration check: \(_input.check)

					Read the parent ticket and its children. You own the current branch
					as the feature integration branch. Delegate implementation to child
					tickets, review each result, dismiss accepted workers to merge them
					into your branch, and run the integration check after merges. Do not
					modify source code yourself. Finish only after all required child
					work is integrated or an obstacle has been clearly reported.
					"""
			}
		},
		{type: "wait", timeout: _input.timeout},
		{type: "evaluate", expect: {is_error: false}},
		// The foreman has already integrated its workers into this branch. Its
		// dismiss is the single merge from the feature branch to the target.
		{type: "dismiss"},
	]
}
