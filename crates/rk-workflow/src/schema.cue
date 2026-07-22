// rat-kingdom workflow schema. Every workflow file defines a top-level
// `workflow:` field conforming to #Workflow; files are evaluated as one CUE
// package with this schema, so violations are CUE unification errors.
//
// Runtime context placeholders usable inside step strings:
//   {{ctx.activeAgent}}   name of the most recently spawned agent
//   {{ctx.activeBranch}}  its branch
//   {{ctx.previousResult}} result text of the last completed wait step
// Inside a for_each task template, per-ticket placeholders also resolve:
//   {{item.id}}    the ticket id (e.g. TKT-7)
//   {{item.title}} the ticket title
//   {{item.body}}  the ticket body
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
	// Step type: "spawn" | "wait" | "evaluate" | "dismiss" | "gate".
	type?: string
	// Spawn steps only: match by role.
	role?: string
}

#Step: #SpawnStep | #WaitStep | #EvaluateStep | #DismissStep | #GateStep | #ForEachStep | #WaitAllStep

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

// Dynamic fan-out: spawn one agent per matching ticket, all in parallel, into
// the fan-out set that a following wait_all joins on. Agent-selection fields
// resolve exactly like a spawn step. The task template binds per-ticket
// placeholders {{item.id}}, {{item.title}}, {{item.body}}; the title defaults
// to the ticket id so the supervisor drives that ticket's status lifecycle.
#ForEachStep: {
	type:  "for_each"
	query: #TicketQuery
	role:  string | *"rat"
	// Named agent profile from `agents` (or global config).
	agent?: string
	// Inline overrides (beat any profile).
	harness?:         string
	model?:           string
	permission_mode?: string
	task: {
		title:        string | *"{{item.id}}"
		description?: string
	}
	// Base/merge-target branch override (each rat still gets its own branch).
	branch?: string
}

// Which tickets a fan-out enumerates. "ready" (the default) means open tickets
// with all dependencies satisfied; any other value filters by that literal
// status. Scope is always the workflow's own repo.
#TicketQuery: {
	status: string | *"ready"
	limit:  int & >0 | *5
}

// Parallel join: block until every agent spawned by the preceding fan-out has
// emitted its harness_result, aggregating them into ctx.previousResult
// ({count, ok, errors, all_ok, results}) for a following evaluate.
#WaitAllStep: {
	type:    "wait_all"
	timeout: string | *"45m"
}
