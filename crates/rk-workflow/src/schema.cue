// rat-kingdom workflow schema. Every workflow file defines a top-level
// `workflow:` field conforming to #Workflow; files are evaluated as one CUE
// package with this schema, so violations are CUE unification errors.
//
// Runtime context placeholders usable inside step strings:
//   {{ctx.activeAgent}}    name of the most recently spawned agent
//   {{ctx.activeBranch}}   its branch
//   {{ctx.previousResult}} result text of the last completed wait step
//   {{ctx.var.<name>}}     a variable lifted by a `read` step
// Parameters are referenced as _input.<name> and resolve at load time.
package workflow

workflow: #Workflow

#Workflow: {
	// Lowercase-hyphen name; also the file stem by convention.
	name:        string & =~"^[a-z][a-z0-9-]*$"
	description: string | *""

	// Declared parameters, referenced via _input.<name>.
	params: [string]: #Param

	// Named agent profiles serving as defaults for this workflow's spawn
	// steps. The profile "default" applies to every spawn step that names no
	// profile. Fields left unset fall through to the global [agents.<name>]
	// config, then to the global default harness.
	agents: [string]: #AgentProfile

	// Steps run in sequence. At least one required.
	steps: [...#Step] & [_, ...]

	// Aspects splice before/after steps around matches at load time.
	aspects?: [...#Aspect]
}

#Param: {
	type:     "string" | "int" | "bool"
	required: bool | *true
	default?: _
}

// Which harness/model runs an agent. All fields optional; resolution is
// field-wise: step > workflow profile > global profile > global defaults.
#AgentProfile: {
	harness?:         "claude" | "codex" | "axe" | "fake"
	model?:           string
	permission_mode?: string
}

#Aspect: {
	match: #AspectMatch
	before?: [...#Step]
	after?: [...#Step]
}

// All non-empty fields must match (AND).
#AspectMatch: {
	// Step type: "spawn" | "wait" | "evaluate" | "dismiss" | "gate" | "read" |
	// "when" | "repeat" | "break" | "stop". Aspects only weave top-level steps,
	// not steps nested inside `when`/`repeat`.
	type?: string
	// Spawn steps only: match by role.
	role?: string
}

#Step: #SpawnStep | #WaitStep | #EvaluateStep | #DismissStep | #GateStep |
	#ReadStep | #WhenStep | #RepeatStep | #BreakStep | #StopStep

// Tuple categories a `read` step may match.
#Category: "fact" | "convention" | "task" | "available" | "claim" | "obstacle" |
	"need" | "artifact" | "event" | "message" | "suggestion" | "endorsement"

#SpawnStep: {
	type: "spawn"
	role: string | *"rat"
	// Named agent profile from `agents` (or global config).
	agent?: string
	// Inline overrides (beat any profile).
	harness?:         string
	model?:           string
	permission_mode?: string
	task: {
		title:        string
		description?: string
	}
	// Base/merge-target branch override.
	branch?: string
}

// Wait for the most recently spawned agent to complete (task_done tuple or
// harness result), capturing its result into ctx.previousResult.
#WaitStep: {
	type:    "wait"
	timeout: string | *"10m"
}

// Unify `expect` with the previous wait's result payload via CUE; the step
// passes iff the unification is valid and concrete.
#EvaluateStep: {
	type:   "evaluate"
	expect: _
}

#DismissStep: {
	type: "dismiss"
	noMerge?: bool
}

// Timer gate only in v1 (human gates arrive with approval tuples).
#GateStep: {
	type:     "gate"
	gateType: "timer"
	duration: string
}

// Lift the newest matching tuple from the space into a ctx variable, so a
// later `when` step can route on it. This is how a reviewer's verdict —
// recorded as an artifact tuple (`rk out artifact <repo> review ...`) —
// becomes observable to the workflow. Blocks up to `timeout` if no tuple
// matches yet; the newest match wins so re-review rounds see the latest verdict.
#ReadStep: {
	type: "read"
	// Tuple category to match.
	category: #Category
	// Tuple identity to match (e.g. "review").
	identity: string
	// Scope to match; defaults to this workflow's repo name at runtime.
	scope?: string
	// Optional substring the serialized payload must contain.
	search?: string
	// JSON payload field to lift (e.g. "recommendation"); whole payload if unset.
	field?: string
	// ctx variable name to store the value under (referenced by `when.var`).
	into:    string
	timeout: string | *"5m"
}

// Route on a ctx variable set by a prior `read`. Runs the sub-steps of the
// matching case, or `default` if the value matches no case. Nested steps are
// executed in place; they share the one-active-agent context.
#WhenStep: {
	type: "when"
	// Name of the ctx variable to switch on (as set by `read`.into).
	var: string
	// value -> steps. String values match by equality.
	cases: [string]: [...#Step]
	default?: [...#Step]
}

// Bounded loop: run `steps` in order up to `max` times. A `break` step inside
// exits early; falling off the end of the body starts the next iteration. The
// hard `max` cap (<=100) preserves the executor's "steps run once" safety
// property — the body can execute at most `max` times, never unbounded.
#RepeatStep: {
	type:  "repeat"
	max:   int & >0 & <=100
	steps: [...#Step] & [_, ...]
}

// Exit the nearest enclosing `repeat` immediately; the instance keeps running
// after the loop. A `break` with no enclosing `repeat` ends the workflow.
#BreakStep: {
	type: "break"
}

// Abort the whole workflow instance (it finishes as failed) with an optional
// reason — the routing target for a reviewer STOP verdict.
#StopStep: {
	type:    "stop"
	reason?: string
}
