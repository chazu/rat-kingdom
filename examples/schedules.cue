// Example scheduler definitions. Copy to `~/.rat-kingdom/schedules/` (global) or
// drop a `schedules.cue` in a repo's `.rk/` directory. Each entry fires a
// workflow on a cron cadence — validated against
// crates/rk-workflow/src/schedules-schema.cue.
//
//   cp examples/schedules.cue ~/.rat-kingdom/schedules/
//
// A scheduled fire is a time-sourced trigger: it reuses the reactor's dispatch
// path (`engine.run`), but the source is a clock, not a matching tuple. Cron is
// evaluated in UTC at minute granularity. Supported syntax per field:
//   *            any value
//   a,b,c        a list
//   a-b          a range
//   */n , a-b/n  a step
// plus the macros @hourly @daily @weekly @monthly @yearly @midnight. Each
// schedule is single-flight (keyed on its name): if its previous run is still
// active, the next fire is skipped, so a slow drain never stacks on itself.
schedules: [
	// Every night at 03:00 UTC, drain the ready backlog in `rat-kingdom`.
	{
		name: "nightly-drain"
		cron: "0 3 * * *"
		run:  "backlog-drain"
		repo: "rat-kingdom"
	},

	// Every hour on the hour, groom the backlog (dedup/triage stale tickets).
	{
		name: "hourly-groom"
		cron: "@hourly"
		run:  "backlog-groom"
		repo: "rat-kingdom"
	},

	// Weekdays at 09:00 UTC, refine agent prompts from the week's transcripts.
	// Params are static strings passed to the workflow verbatim.
	{
		name: "weekday-prompt-refine"
		cron: "0 9 * * 1-5"
		run:  "prompt-refine"
		repo: "rat-kingdom"
		params: {window: "7d"}
	},
]
