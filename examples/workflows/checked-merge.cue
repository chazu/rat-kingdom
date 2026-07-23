// checked-merge: one rat implements a task, then the repo's OWN test/lint
// suite runs deterministically in that rat's worktree — and only a green suite
// lands. This is the quality primitive that makes auto-merge trustworthy: where
// `evaluate` alone unifies against the harness's self-reported output (it takes
// the rat's word), the `run` step executes the real checks and captures a
// verdict {exit,stdout,stderr} the runner cannot forge. A red suite fails the
// instance closed, so the branch is held unmerged.
//
//   rk workflow run checked-merge --param taskId=fix-login \
//     --param description="Fix the login redirect loop" \
//     --param check="cargo test --quiet"
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "checked-merge"
	description: "spawn one rat, run the repo's real checks in its worktree, merge only if green"

	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		// The repo's real check command, run verbatim in the rat's worktree.
		check: {type: "string", required: false, default: "cargo test"}
	}

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
		// The rat says it finished cleanly...
		{type: "evaluate", expect: {is_error: false}},
		// ...now PROVE it: run the repo's real suite in the rat's worktree. The
		// {exit,stdout,stderr} lands in ctx.previousResult for the evaluate below.
		{type: "run", command: _input.check, timeout: "20m"},
		// Hard, deterministic gate: a non-zero exit fails the instance closed,
		// so the dismiss never runs and the branch is never merged.
		{type: "evaluate", expect: {exit: 0}},
		// Green suite: merge the rat's branch into the base and clean up.
		{type: "dismiss"},
	]
}
