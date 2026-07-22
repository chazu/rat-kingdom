// solo-task: one rat implements a task, its result is checked, and its
// branch is auto-merged into the base branch on success.
//
//   rk workflow run solo-task --param taskId=fix-login \
//     --param description="Fix the login redirect loop"
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "solo-task"
	description: "spawn one rat, wait, verify success, merge"

	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
	}

	// Workflow-wide defaults; override globally via [agents.default] in
	// config.toml, or per-run by editing this profile.
	agents: {
		default: {harness: "claude"}
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
		// Full CUE semantics: the harness result must not be an error.
		{type: "evaluate", expect: {is_error: false}},
		// Merges the rat's branch into the base branch and cleans up.
		{type: "dismiss"},
	]
}
