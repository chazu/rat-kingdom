// fix-then-verify: a fixer rat implements a change, then an independent
// verifier rat writes a regression test and confirms it goes red→green before
// the fix and its test are merged into the base branch.
//
//   rk workflow run fix-then-verify --param taskId=fix-login \
//     --param description="Fix the login redirect loop"
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "fix-then-verify"
	description: "fixer implements; independent verifier writes a regression test and confirms red→green"
	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
	}
	agents: {
		default:  {harness: "claude"}
		verifier: {harness: "claude"}
	}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {title: _input.taskId, description: _input.description}
		},
		{type: "wait", timeout: "30m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},          // keep branch for the verifier
		{
			type:  "spawn"
			role:  "verifier"
			agent: "verifier"
			task: {
				title: "verify-" + _input.taskId
				description: """
					You are on branch {{ctx.activeBranch}}. The task was: \(_input.description)
					Independently verify the fix:
					  1. Write or identify a regression test that FAILS on main.
					  2. Confirm it PASSES on this branch.
					  3. Run the full suite.
					Commit the test. If verification fails, exit non-zero (is_error).
					Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		{type: "evaluate", expect: {is_error: false}},   // hard gate: bad verify fails the run
		{type: "dismiss"},                                // merge fix + regression test
	]
}
