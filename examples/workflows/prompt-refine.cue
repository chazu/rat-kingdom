// prompt-refine: one rat mines obstacle/need tuples and failed runs, then
// proposes edits to role prompts and convention tuples (proposals only — it
// does not touch live prompts). Its proposals are merged on success.
//
//   rk workflow run prompt-refine
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "prompt-refine"
	description: "mine obstacle/need tuples and failed runs; propose prompt + convention edits"
	agents: {default: {
		harness: "claude"
		// This workflow must run rk, git, and repo checks unattended. Keep the
		// broad permission explicit here rather than changing the fleet default.
		permission_mode: "bypassPermissions"
	}}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "refine-prompts"
				description: """
					Scan recurring pain:
					  rk scan obstacle ; rk scan need ; rk scan fact system
					Cross-reference with recent workflow_failed events.
					Where a recurring failure traces to a weak role prompt or a missing
					convention, propose a concrete edit (write a diff/patch under
					docs/proposals/prompts/) and, for durable rules, propose a convention:
					  rk out artifact $RK_REPO convention-proposal --payload '{"rule": "...", "why": "..."}'
					File a ticket for each proposed change. Do NOT edit live prompts.
					Commit proposals, report done.
					"""
			}
		},
		// Mining the full event feed and writing proposals has exceeded 25m in
		// production. Give this single self-improvement task the fan-out budget.
		{type: "wait", timeout: "45m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss"},
	]
}
