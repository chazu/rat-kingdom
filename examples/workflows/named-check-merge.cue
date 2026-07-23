// named-check-merge: the hardened twin of checked-merge (TKT-30). One rat
// implements a task, then a repo-REGISTERED named check runs deterministically
// in its worktree — and only a green check lands. Where checked-merge carries a
// raw `command` (only as trusted as the workflow def that ships it), this step
// references a check BY NAME from <repo>/.rk/checks.cue, the repo owner's own
// allowlist. With `[policy] require_named_checks = true` a raw command is refused
// fail-closed, so a compromised workflow def can invoke only the repo's checks —
// never arbitrary shell.
//
//   # register the check once, in the repo:
//   #   <repo>/.rk/checks.cue  (see examples/checks.cue) — e.g. name: "test"
//   rk workflow run named-check-merge --param taskId=fix-login \
//     --param description="Fix the login redirect loop"
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "named-check-merge"
	description: "spawn one rat, run a repo-registered named check in its worktree, merge only if green"

	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		// Name of a check registered in <repo>/.rk/checks.cue.
		check: {type: "string", required: false, default: "test"}
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
		// ...now PROVE it: run the NAMED check in the rat's worktree. Its command,
		// timeout, and any expectExit come from the repo's checks registry. The
		// {exit,stdout,stderr} lands in ctx.previousResult for the evaluate below.
		{type: "run", check: _input.check},
		// Hard, deterministic gate: a non-zero exit fails the instance closed,
		// so the dismiss never runs and the branch is never merged.
		{type: "evaluate", expect: {exit: 0}},
		// Green check: merge the rat's branch into the base and clean up.
		{type: "dismiss"},
	]
}
