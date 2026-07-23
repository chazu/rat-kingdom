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
	// ── Recommended: the whole overnight self-improvement chain as ONE fire ──
	// Every night at 03:00 UTC, groom the backlog, drain it in parallel, then
	// propose prompt/convention refinements — a single instance behind a single
	// single-flight lock, so a slow drain can never let the next night's groom
	// stack on top of it. Overnight cost is bounded by the fleet/repo budget caps.
	{
		name: "nightly-self-improve"
		cron: "0 3 * * *"
		run:  "nightly-self-improve"
		repo: "rat-kingdom"
		// Optional: cap the drain and its join window.
		params: {limit: "5", timeout: "45m"}
	},

	// ── Or schedule the three phases separately ─────────────────────────────
	// Use these INSTEAD of nightly-self-improve when you want each phase to be
	// independently retryable (each gets its own single-flight lock and cadence),
	// rather than one all-or-nothing nightly instance. Don't run both — you'd
	// groom/drain/refine twice.
	//
	// {
	//     name: "hourly-groom"
	//     cron: "@hourly"
	//     run:  "backlog-groom"
	//     repo: "rat-kingdom"
	// },
	// {
	//     name: "nightly-drain"
	//     cron: "0 3 * * *"
	//     run:  "backlog-drain"
	//     repo: "rat-kingdom"
	// },
	// {
	//     name: "weekday-prompt-refine"
	//     cron: "0 9 * * 1-5"
	//     run:  "prompt-refine"
	//     repo: "rat-kingdom"
	//     params: {window: "7d"}
	// },
]
