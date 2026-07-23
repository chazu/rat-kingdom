// backlog-drain: fan out one solo-task rat per ready ticket, run them in
// parallel, then join and report — the operator's throughput dial. Pairs with
// backlog-groom (which keeps the queue decomposed) to turn a well-groomed
// backlog directly into parallel fleet work.
//
//   rk workflow run backlog-drain
//   rk workflow run backlog-drain --param limit=3
//
// The `for_each` step enumerates ready tickets (open, dependencies satisfied)
// in this repo and spawns one rat per ticket with the ticket id as its task
// title — so the supervisor drives each ticket's status lifecycle (→ done on a
// clean finish, → closed on merge), exactly as for any ticket-dispatched rat.
// `wait_all` blocks until every rat has finished, aggregating their results
// into ctx.previousResult ({count, ok, errors, all_ok, results}); the evaluate
// then asserts none errored. Finally `dismiss_all` merges every rat's branch in
// one parallel sweep and clears the fan-out set — the symmetric close to the
// fan-out (for_each spawns them, wait_all joins them, dismiss_all merges them).
// Drop the trailing dismiss_all to leave branches parked for manual merge.
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "backlog-drain"
	description: "fan out one rat per ready ticket, run in parallel, join and report"

	params: {
		// Cap on how many ready tickets to drain in one pass.
		limit: {type: "int", required: false, default: 5}
		// Join timeout — every rat must finish within this window.
		timeout: {type: "string", required: false, default: "45m"}
		// Per-instance wallet cap in whole USD. Once this run's spawned agents'
		// summed cost reaches it, further dispatch is refused (below the global
		// fleet/repo caps). (max_usd is a number; params are int/string/bool
		// only, so this is an int-dollar param.)
		budgetUsd: {type: "int", required: false, default: 10}
	}

	agents: {default: {harness: "claude"}}

	// Wallet kill-switch scoped to this one drain instance.
	budget: {max_usd: _input.budgetUsd}

	steps: [
		{
			type: "for_each"
			query: {status: "ready", limit: _input.limit}
			role: "rat"
			task: {
				// title defaults to {{item.id}} (the ticket id), which the
				// supervisor uses to close that ticket's loop on completion.
				title: "{{item.id}}"
				description: """
					Work ticket {{item.id}}: {{item.title}}

					{{item.body}}

					This is your only task. Implement it end-to-end on your branch,
					commit your work, run the repo's tests, then run `rk done`.
					"""
			}
		},
		{type: "wait_all", timeout: _input.timeout},
		// Every drained rat must have finished cleanly.
		{type: "evaluate", expect: {all_ok: true}},
		// Merge every rat's branch in parallel and clear the fan-out set.
		{type: "dismiss_all"},
	]
}
