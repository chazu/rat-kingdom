// rat-kingdom workflow schema. Every workflow file defines a top-level
// `workflow:` field conforming to #Workflow; files are evaluated as one CUE
// package with this schema, so violations are CUE unification errors.
//
// Runtime context placeholders usable inside step strings:
//   {{ctx.activeAgent}}    name of the most recently spawned agent
//   {{ctx.activeBranch}}   its branch
//   {{ctx.previousResult}} result text of the last completed wait step
//   {{ctx.var.<name>}}     a variable lifted by a `read` step
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

	// Cost-tier routing: map ticket labels/priority to a tier (an agent profile
	// name) for this workflow's fan-out spawns. Takes precedence over the global
	// [tiers] table; see #TierRouting.
	tiers?: #TierRouting

	// Per-instance budget cap. Once the SUM of this instance's spawned agents'
	// cost reaches max_usd, further dispatch (single spawn or fan-out) is
	// refused — the wallet kill-switch scoped to one workflow run, layered
	// below the global fleet/repo caps.
	budget?: #WorkflowBudget

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

// Per-workflow-instance budget cap. Positive USD ceiling on the summed cost of
// every agent this instance spawns.
#WorkflowBudget: {
	max_usd: number & >0
}

// Which harness/model runs an agent. All fields optional; resolution is
// field-wise: step > workflow profile > global profile > global defaults.
#AgentProfile: {
	harness?:         "claude" | "codex" | "axe" | "fake"
	model?:           string
	permission_mode?: string
}

// Cost-tier routing table: an ordered list of rules mapping a ticket's
// labels/priority to a tier — the name of an agent profile (`agents.<tier>`
// here, or global `[agents.<tier>]`). First matching rule wins.
#TierRouting: {
	rules: [...#TierRule]
}

// One routing rule. `priority`/`label` are AND'd; either unset means "any".
// Both unset is an unconditional catch-all (handy as the last rule).
#TierRule: {
	priority?: string
	label?:    string
	tier:      string
}

#Aspect: {
	match: #AspectMatch
	before?: [...#Step]
	after?: [...#Step]
}

// All non-empty fields must match (AND).
#AspectMatch: {
	// Step type: "spawn" | "wait" | "evaluate" | "dismiss" | "gate" | "read" |
	// "when" | "repeat" | "break" | "stop" | "for_each" | "wait_all" |
	// "dismiss_all" | "run" | "land". Aspects only weave top-level steps, not
	// steps nested inside `when`/`repeat`.
	type?: string
	// Spawn steps only: match by role.
	role?: string
}

#Step: #SpawnStep | #WaitStep | #EvaluateStep | #DismissStep | #GateStep |
	#ReadStep | #WhenStep | #RepeatStep | #BreakStep | #StopStep |
	#ForEachStep | #WaitAllStep | #DismissAllStep | #RunStep | #LandStep

// Tuple categories a `read` step may match.
#Category: "fact" | "convention" | "task" | "available" | "claim" | "obstacle" |
	"need" | "artifact" | "resolution" | "event" | "message" | "suggestion" | "endorsement"

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

// A gate parks the workflow between steps. Two kinds:
//   timer    — sleep for `duration`, then continue (schedule, not consent).
//   approval — block until a human decision arrives for THIS instance (via
//              `rk approve <instance>` / `rk reject <instance>`) or `timeout`
//              elapses. The decision ({approved: bool, by, reason}) lands in
//              ctx.previousResult so a following `evaluate` can gate the merge.
//              On timeout with no response the decision is {approved: false}.
#GateStep: {
	type:     "gate"
	gateType: "timer" | "approval"
	// timer only: how long to sleep.
	if gateType == "timer" {
		duration: string
	}
	// approval only: how long to wait for a human before defaulting to
	// not-approved. The safety valve fails closed.
	if gateType == "approval" {
		timeout: string | *"24h"
	}
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

// Parallel dismiss: for every agent in the fan-out set, dismiss it (merge its
// branch unless noMerge) concurrently, then clear the fan-out set. This is the
// fan-out counterpart to a single `dismiss` over active_agent — where dismiss
// merges the one active branch, dismiss_all merges every branch a preceding
// for_each parked. The aggregate ({count, merged, errors, all_merged, results})
// lands in ctx.previousResult for a following evaluate.
#DismissAllStep: {
	type: "dismiss_all"
	noMerge?: bool
	// When true, merge only the branches of rats that finished clean
	// (is_error:false in the preceding wait_all) and park the rest with
	// noMerge, instead of failing the batch on the first error. Requires a
	// preceding wait_all in the same instance (its per-agent results supply
	// the clean/failed signal). Default false = atomic-batch (today).
	onlyClean?: bool
}

// Run a command in the active agent's worktree — the deterministic quality
// gate. Where `evaluate` unifies only against the harness's self-reported
// output (it takes the rat's word), `run` executes the repo's real test/lint
// suite and captures {exit, stdout, stderr} into ctx.previousResult, so a
// following `evaluate {expect: {exit: 0}}` (or a `when`) can gate the merge on
// a verdict the runner cannot forge. A non-zero exit fails closed: either via
// the following evaluate, or inline when `expectExit` is set.
//
// SECURITY: a raw `command` is executed verbatim via `sh -c` in the worktree and
// is only as trusted as the workflow definition that carries it. With the
// `[policy] require_named_checks` flag on, a raw `command` is REFUSED fail-closed
// and the step must instead reference a `check` — a repo-owned named entry in
// `<repo>/.rk/checks.cue` (TKT-30). A named check runs regardless of the policy,
// so a compromised workflow def can invoke only the repo's registered checks,
// never arbitrary shell. Exactly one of `command` / `check` is set.
#RunStep: {
	type: "run"
	// Raw command line, run via `sh -c` in the worktree. Gated by the
	// require_named_checks policy. Mutually exclusive with `check`.
	command?: string
	// Name of a repo-registered check (`<repo>/.rk/checks.cue`) to run instead of
	// a raw command. Runs regardless of policy. Mutually exclusive with `command`.
	check?: string
	// Working directory relative to the worktree root; the root if unset. For a
	// named check, overrides the check's own cwd when set.
	cwd?: string
	// If set, the step fails the instance inline when the actual exit code
	// differs (fail-closed). If unset, the exit is only captured for a
	// following evaluate/when to route on. For a named check, overrides the
	// check's own expectExit when set.
	expectExit?: int
	// Hard wall-clock bound; a suite still running when it elapses is killed
	// and the step fails closed. A named check's own timeout applies unless the
	// step overrides it.
	timeout: string | *"10m"
}

// Merge a NAMED branch into a NAMED target directly — "land" the work. Where
// `dismiss` merges the active agent's branch into ITS OWN base, `land` names
// both source `branch` and merge `target`, so an APPROVE verdict lands reviewed
// work straight onto (e.g.) main with no human doing the final merge. This
// closes the last manual hop when a reviewer is chained off a work branch — its
// dismiss can only merge into that base, never main.
//
// Both fields interpolate {{ctx.*}}: `branch: "{{ctx.activeBranch}}"` lands the
// branch the workflow is holding. The merge is CAS-safe (a detached worktree;
// the target ref advances only if it did not move), so it disturbs no live
// checkout and fails safe under concurrency: a merge conflict or a moved target
// is a clean {merged: false} in ctx.previousResult ({branch, target, merged,
// detail, branch_deleted}), NOT an error — gate on it with a following
// `evaluate {expect: {merged: true}}` or a `when`. On a successful merge the
// source branch is deleted unless `keepBranch` (a protected or still-checked-out
// branch is left in place and reported not deleted).
//
// SAFETY: `land` merges with no review of its own. Reach it only through an
// APPROVE `when`-branch or after an approval gate — never as an unconditional
// step — or unreviewed work lands. A hard policy restriction (and merge-queue
// serialization) is deferred to the policy engine.
#LandStep: {
	type:   "land"
	branch: string
	target: string
	keepBranch?: bool
}
