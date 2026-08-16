// rat-kingdom #Trigger schema. A trigger file defines a top-level `triggers:`
// list; each entry names a match predicate over a tuple and the workflow to run
// when a newly-landed tuple matches. Files are evaluated as one CUE package with
// this schema, so violations are CUE unification errors — exactly like workflows.
//
// The workflow is launched with params templated from the matched tuple:
//   {{tuple.category}} {{tuple.scope}} {{tuple.identity}} {{tuple.instance}}
//   {{tuple.id}}       — the tuple's structural fields
//   {{tuple.payload.<key>}} — a top-level payload field. When a param's whole
//     value is a single such placeholder the raw JSON value (and its type) is
//     passed through; otherwise it is string-interpolated.
package triggers

triggers: [...#Trigger]

#Trigger: {
	// Lowercase-hyphen name; unique per file by convention.
	name: string & =~"^[a-z][a-z0-9-]*$"

	// Predicate a landing tuple must satisfy (all set fields AND). Empty fields
	// match anything — matched with the same Pattern::matches used everywhere.
	match: #TriggerMatch

	// Action this trigger performs on a match. "workflow" (default) launches
	// the named `run` workflow through the workflow engine. "land" enqueues
	// the match directly onto the daemon-native LandingPipeline (Phase 3-T4,
	// docs/proposals/daemon-native-landing-pipeline.md §2.1 option (a))
	// instead of spawning a workflow instance — `run` is not read for this
	// action, only `match`/`repo`/`exclude`/`maxFires` are.
	action?: "workflow" | "land"

	// Workflow to run on a match (a definition name resolvable in the target
	// repo's .rk/workflows or the global workflows dir). Required when
	// `action` is "workflow" (the default) — enforced by the reactor at
	// dispatch time, not by this schema, so a "land" trigger need not name
	// one at all.
	run?: string & =~"^[a-z][a-z0-9-]*$"

	// Registered repo the workflow runs in. Defaults to the matched tuple's
	// scope (repo-scoped tuples), or — for a repo-local trigger file — the repo
	// it was discovered in.
	repo?: string

	// Params passed to the workflow, each templated from the matched tuple.
	params?: [string]: string

	// Tuple authors this trigger never reacts to (re-entrancy break), on top of
	// the reactor's own output which is always excluded.
	exclude?: [...string]

	// Per-trigger fire cap within the reactor's rolling window. Bounded to <=100
	// to mirror the `repeat` max discipline — a storm can never run unbounded.
	maxFires?: int & >0 & <=100

	// Cap on this trigger's concurrently in-flight (Running) workflow instances.
	// Beyond the cap a match is durably QUEUED — never dropped, never run
	// unbounded — and dispatched, oldest first, the moment an earlier instance
	// completes and frees a slot. Unset means no cap (legacy, unbounded
	// concurrency). Bounded to <=100 to mirror `maxFires`.
	maxInFlight?: int & >0 & <=100
}

#TriggerMatch: {
	category?: #Category
	scope?:    string
	identity?: string
	instance?: string
	// Substring the serialized payload must contain.
	search?: string
}

#Category: "fact" | "convention" | "task" | "available" | "claim" | "obstacle" |
	"need" | "artifact" | "resolution" | "event" | "message" | "suggestion" | "endorsement" | "fact_vote"
