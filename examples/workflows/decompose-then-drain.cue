// decompose-then-drain: the auto-decompose→drain macro, expressed as ONE
// composition instead of a hand-copy of another workflow's steps. It runs two
// phases back to back:
//
//   1. DECOMPOSE — a rat grooms the backlog: split oversized tickets into
//                  sub-tickets, merge duplicates, tag stale items. This mutates
//                  the ticket store (nothing to merge), so it dismisses noMerge.
//   2. DRAIN     — COMPOSE the shipped `backlog-drain` workflow via a single
//                  `sub_workflow` step. Rather than copying backlog-drain's
//                  for_each → wait_all → evaluate → dismiss_all body into this
//                  file (and letting the two drift), we run it by name. Its
//                  result (`{count, ok, errors, all_ok, results}` from its
//                  dismiss_all) joins into this workflow's ctx.previousResult,
//                  so the trailing `evaluate` gates on how the drain finished.
//
// This is the composition primitive (#SubWorkflowStep, TKT-57): a `sub_workflow`
// step runs another workflow inline — resolved exactly like `rk workflow run`
// (repo-local `.rk/workflows/` over the global dir) — to completion, then joins
// its result back. `params` are templated at run time; forward a parent param
// with CUE interpolation, `params: {limit: "\(_input.limit)"}`. Nesting is
// bounded by a hard depth cap, so a workflow cycle fails closed.
//
//   rk workflow run decompose-then-drain
//   rk workflow run decompose-then-drain --param limit=3
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/. Requires
// `backlog-drain` to be resolvable in the same location.
workflow: {
	name:        "decompose-then-drain"
	description: "groom/decompose the backlog, then drain it by composing backlog-drain"

	params: {
		// Cap on how many ready tickets the composed drain works in one pass.
		limit: {type: "int", required: false, default: 5}
		// Per-instance wallet cap (whole USD) forwarded to the composed drain.
		budgetUsd: {type: "int", required: false, default: 10}
	}

	agents: {default: {harness: "claude"}}

	steps: [
		// ── Phase 1: DECOMPOSE ──────────────────────────────────────────────
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "decompose-backlog"
				description: """
					Read the open backlog:  rk ticket list --status open
					For each oversized ticket, decompose it:
					  rk ticket new "<sub>" --parent <TKT-id>
					Merge duplicates (note the survivor), flag stale items. Do NOT
					start any ticket. Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		// Grooming mutates the ticket store, not the worktree — nothing to merge.
		{type: "dismiss", noMerge: true},

		// ── Phase 2: DRAIN (composed) ───────────────────────────────────────
		// Run backlog-drain by name; forward this workflow's params into it (CUE
		// stringifies the ints at load, the child coerces them back). Its result
		// lands in ctx.previousResult for the evaluate below.
		{
			type:     "sub_workflow"
			workflow: "backlog-drain"
			params: {
				limit:     "\(_input.limit)"
				budgetUsd: "\(_input.budgetUsd)"
			}
		},
		// Gate on how the composed drain finished: every drained rat merged.
		{type: "evaluate", expect: {all_merged: true}},
	]
}
