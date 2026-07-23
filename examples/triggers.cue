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

	// THE STEWARD (leverage #2). On every rat completion, reactively triage that
	// rat's branch: a cheap reviewer + the repo's real gate + a protected-path
	// policy check decide auto-merge / rework-ticket / escalate — so the operator
	// reviews exceptions, not every branch.
	//
	// `"role":"rat"` scopes the fire to plain-rat completions, which is also the
	// re-entrancy break: the reviewer the steward spawns completes as a
	// "reviewer" (not "rat"), so its own harness_result never re-triggers the
	// steward on the branch it just reviewed. (`is_error` is NOT filtered here —
	// an errored rat's branch is still triaged, and its broken work is caught by
	// the run gate and held unmerged, not merged.)
	//
	// NOTE: installing this makes ALL rat completions auto-merge on a clean
	// verdict. Do not also run an approval-gated workflow (land-on-approve) over
	// the same completions, or the two race for the branch.
	{
		name:  "steward-on-completion"
		match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
		run:   "steward"
		params: {
			// String-interpolated (always a string) so a taskless rat still loads.
			taskId: "{{tuple.payload.task}}"
			// Raw pass-through: the exact branch the reviewer must chain onto.
			branch: "{{tuple.payload.branch}}"
			// The repo the completion is scoped to.
			repo: "{{tuple.scope}}"
		}
		// A completion storm must not spawn a steward storm; each fire is still
		// idempotent per completion tuple, this caps the rolling window.
		maxFires: 20
	},
]
