// rat-kingdom workflow schema. Every workflow file defines a top-level
// `workflow:` field conforming to #Workflow; files are evaluated as one CUE
// package with this schema, so violations are CUE unification errors.
//
// Runtime context placeholders usable inside step strings:
//   {{ctx.activeAgent}}   name of the most recently spawned agent
//   {{ctx.activeBranch}}  its branch
//   {{ctx.previousResult}} result text of the last completed wait step
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

#Step: #SpawnStep | #WaitStep | #EvaluateStep | #DismissStep | #GateStep

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
