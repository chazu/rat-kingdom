// Example reactor triggers. Copy to `~/.rat-kingdom/triggers/` (global) or drop
// a `triggers.cue` in a repo's `.rk/` directory. Each entry fires a workflow
// when a matching tuple lands in the space — validated against
// crates/rk-workflow/src/triggers-schema.cue.
//
//   cp examples/triggers.cue ~/.rat-kingdom/triggers/
//
// Template params from the matched tuple:
//   {{tuple.category}} {{tuple.scope}} {{tuple.identity}} {{tuple.instance}}
//   {{tuple.id}}       {{tuple.payload.<field>}}
// A param whose whole value is one {{tuple.payload.<field>}} placeholder passes
// the raw JSON value (and type) through; otherwise it is string-interpolated.
triggers: [
	// When any rat records a blocking obstacle, kick off a triage workflow in
	// the repo the obstacle is scoped to (its scope resolves to a registered
	// repo), passing the obstacle text along.
	{
		name:  "triage-obstacle"
		match: {category: "obstacle"}
		run:   "solo-task"
		params: {
			taskId:      "triage-{{tuple.identity}}"
			description: "Investigate the reported obstacle: {{tuple.payload.text}}"
		}
		// Never react to the daemon's own bookkeeping obstacles.
		exclude: ["daemon"]
		// A pile-up of obstacles must not spawn a pile-up of triage rats.
		maxFires: 5
	},

	// When a ticket is created in `rat-kingdom`, drain the ready backlog.
	{
		name:  "drain-on-ticket"
		match: {category: "event", identity: "ticket_created", scope: "rat-kingdom"}
		run:   "backlog-drain"
		repo:  "rat-kingdom"
		maxFires: 3
	},
]
