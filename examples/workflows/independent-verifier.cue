// independent-verifier: a rat implements the task, then an independent verifier
// examines the acceptance criteria against collected evidence and records a
// recommendation. The verifier has NO implementation authority — it only reads
// the branch, checks evidence, and reports satisfied criteria and gaps. Nothing
// auto-merges; a human reads the verification report and decides.
//
//   rk workflow run independent-verifier --param taskId=add-caching \
//     --param description="Add response caching to the API layer"
//
// The verifier runs `rk product-to-code verify-report validate` to map every
// acceptance criterion to evidence (or an explicit gap) and lands the report as
// an artifact tuple: rk scan artifact
workflow: {
	name:        "independent-verifier"
	description: "rat implements, independent verifier checks evidence and gaps, human decides"

	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		verifyTimeout: {type: "string", required: false, default: "15m"}
	}

	agents: {
		// Implementation gets the strong model, verification runs cheaper.
		default: {harness: "claude"}
		verifier: {harness: "claude", model: "haiku"}
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
		// Keep the branch: the verifier needs it, and the human decides later.
		{type: "dismiss", noMerge: true},
		{
			type:  "spawn"
			role:  "verifier"
			agent: "verifier"
			task: {
				title: "verify-" + _input.taskId
				description: """
					You are the independent verifier for task: \(_input.description)

					Do not modify implementation code. You have no implementation
					authority: you only read the branch ({{ctx.activeBranch}}),
					inspect collected evidence, and report satisfied criteria and gaps.

					1. Read the initiative and its acceptance criteria.
					2. Gather evidence into an evidence directory (test runs, browser
					   acceptance, review artifacts).
					3. Write a verification report mapping every acceptance criterion
					   to evidence or an explicit gap, then validate it:

					   rk product-to-code verify-report validate \\
					     --report report.json \\
					     --initiative initiative.json \\
					     --evidence-dir evidence/

					4. Record the verification report as an artifact so the human sees
					   evidence and gaps before deciding to merge:
					   rk out artifact $RK_REPO verification --payload '{"task": "\(_input.taskId)", "recommendation": "deliver|hold", "notes": "..."}'
					"""
			}
		},
		{type: "wait", timeout: _input.verifyTimeout},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},
	]
}
