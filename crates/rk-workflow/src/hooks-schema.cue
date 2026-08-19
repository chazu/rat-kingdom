// rat-kingdom #Hook schema. A hook file defines a top-level `hooks:` list;
// each entry names the lifecycle events it reacts to and the program to run
// on a match. Files are evaluated as one CUE package with this schema, so
// violations are CUE unification errors — exactly like triggers.
//
// Hooks are operator-authored automation, not agent-writable state: a
// castle-level hook lives at `<home>/hooks/*.cue`, a file no rat has
// filesystem or RPC access to; a repo-level hook lives at
// `<repo>/.rk/hooks.cue`, read from the registered checkout the daemon
// advances only on a landed (reviewed, merged) branch — the same trust
// boundary `.rk/triggers.cue` already relies on.
//
// The event tuple lands on the program's stdin as JSON, plus RK_HOOK_* env
// (see docs/reactor.md "Lifecycle hooks"). A hook failure is always logged
// and rate-capped-announced; it can never fail the triggering operation.
package hooks

hooks: [...#Hook]

#Hook: {
	// Lowercase-hyphen name; unique per file by convention.
	name: string & =~"^[a-z][a-z0-9-]*$"

	// Lifecycle events this hook reacts to. At least one required.
	events: [...#Event] & [_, ...]

	// Program to run — exec'd directly, not a shell line (same discipline as
	// `[[notify.sinks]]`'s command sink: anything needing pipes, globbing or
	// quoting belongs in a script this points at).
	command: string

	// Bound on the child before it is killed. Defaults to 10s when unset.
	timeoutSecs?: int & >0

	// Registered repo this hook is scoped to (fires only for that repo's
	// events). Unset for a castle-level hook, which fires for every repo. A
	// repo-local hook file defaults this to the repo it was discovered in.
	repo?: string
}

// The initial lifecycle-hook event vocabulary.
#Event: "agent_spawned" | "agent_completed" | "agent_failed" | "agent_dismissed" |
	"branch_landed" | "gate_failed" | "escalation_raised"
