// backlog-groom: one rat decomposes oversized tickets, dedupes, and tags the
// ticket backlog, improving the queue that feeds every other workflow. It does
// not start any ticket — grooming mutates the ticket store, not the worktree.
//
//   rk workflow run backlog-groom
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "backlog-groom"
	description: "one rat decomposes, dedupes, and tags the ticket backlog"
	agents: {default: {harness: "claude"}}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "groom-backlog"
				description: """
					Read the open backlog:  rk ticket list --status open
					For each oversized ticket, decompose it:
					  rk ticket new "<sub>" --parent <TKT-id>
					Merge duplicates (note the survivor in each), and flag stale items.
					Do NOT start any ticket. Record what you changed:
					  rk out artifact $RK_REPO backlog-groom --payload '{"decomposed": N, "deduped": M}'
					Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},   // no code change; ticket store mutated directly
	]
}
