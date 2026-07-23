// cost-tiered-drain: backlog-drain, but each ready ticket is routed to a cost
// tier from its labels/priority — cheap/fast harness for bounded mechanical
// work, a premium harness for the hard tickets. Cost is the ceiling on how wide
// the fleet runs, so the same backlog drains at a fraction of the spend and the
// autoscaler fits more rats under a fixed budget (TKT-26).
//
//   rk workflow run cost-tiered-drain --param repo=rat-kingdom
//
// How routing works: the `for_each` enumerates ready tickets and, for each, the
// `tiers` table below maps its labels/priority to a tier — the name of an agent
// profile in `agents:` here (or a global `[agents.<tier>]`). First matching
// rule wins; a rule with neither `priority` nor `label` is an unconditional
// fallback. The selected tier resolves just below inline overrides, so it beats
// the workflow's `default` profile but an inline `harness:`/`model:` on the step
// would still win. A workflow's own `tiers:` shadow the global `[tiers]` table.
//
// Escalation-on-failure is expressible on top of this without new machinery:
// drain cheap, `wait_all`, and on an `evaluate` failure re-run the hard tickets
// on the premium tier — one cheap attempt, never a bad merge.
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "cost-tiered-drain"
	description: "fan out one rat per ready ticket, each routed to a cost tier by its labels/priority"

	params: {
		repo: {type: "string", required: false, default: ""}
		limit: {type: "int", required: false, default: 5}
		timeout: {type: "string", required: false, default: "45m"}
	}

	// Two cost tiers plus the fallthrough default. Cheap runs the bounded axe
	// harness; premium runs claude for the hard tickets.
	agents: {
		default: {harness: "claude"}
		cheap:   {harness: "axe"}
		premium: {harness: "claude", model: "opus"}
	}

	// Ordered routing rules; first match wins.
	tiers: {
		rules: [
			// Mechanical/chore tickets → the cheap tier regardless of priority.
			{label: "mechanical", tier: "cheap"},
			{label: "chore", tier: "cheap"},
			// Anything explicitly high priority → premium.
			{priority: "high", tier: "premium"},
			// Everything else → cheap, keeping the default spend low.
			{tier: "cheap"},
		]
	}

	steps: [
		{
			type: "for_each"
			query: {status: "ready", limit: _input.limit}
			role: "rat"
			task: {
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
		{type: "evaluate", expect: {all_ok: true}},
		{type: "dismiss_all"},
	]
}
