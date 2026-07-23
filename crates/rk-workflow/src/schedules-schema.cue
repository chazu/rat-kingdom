// rat-kingdom #Schedule schema. A schedule file defines a top-level
// `schedules:` list; each entry names a cron cadence and the workflow to run on
// it. Files are evaluated as one CUE package with this schema, so violations are
// CUE unification errors — exactly like workflows and triggers.
//
// A scheduled fire is a time-sourced trigger: on cadence the daemon scheduler
// launches `run` with the static `params`, guarded single-flight so a slow run
// never stacks on itself. Unlike a trigger there is no matched tuple, so params
// are plain strings passed through as-is.
package schedules

schedules: [...#Schedule]

#Schedule: {
	// Lowercase-hyphen name; unique per file by convention. Doubles as the
	// single-flight key, so two schedules must not share a name.
	name: string & =~"^[a-z][a-z0-9-]*$"

	// A 5-field cron expression (`minute hour day-of-month month day-of-week`)
	// or a `@macro` (`@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`,
	// `@midnight`). Evaluated in UTC at minute granularity. The full syntax
	// (`*`, lists `a,b`, ranges `a-b`, steps `*/n` and `a-b/n`) is validated in
	// the daemon, not here — a malformed expression is logged and skipped.
	cron: string & !=""

	// Workflow to run on cadence (a definition name resolvable in the target
	// repo's .rk/workflows or the global workflows dir).
	run: string & =~"^[a-z][a-z0-9-]*$"

	// Registered repo the workflow runs in. Defaults to the repo a repo-local
	// schedule file was discovered in; a global schedule file MUST set it, or
	// the fire is skipped (there is no tuple scope to fall back on).
	repo?: string

	// Static params passed to the workflow verbatim (each a string value).
	params?: [string]: string
}
