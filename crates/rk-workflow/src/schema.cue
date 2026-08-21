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
//   {{item.id}}    the ticket id (e.g. TKT-<id>)
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

	// Per-instance override of the daemon's stale-`Running`-instance hard
	// timeout (`[instance_timeout_sweep] default_timeout_secs`, 12h by
	// default). Set this on a workflow expected to legitimately run longer
	// than the fleet-wide default, so the sweep does not fail it out from
	// under itself. A duration string like "24h" or "90m".
	staleTimeout?: string

	// Steps run in sequence. At least one required.
	steps: [...#Step] & [_, ...]

	// Aspects splice before/after steps around matches at load time.
	aspects?: [...#Aspect]
}

#Param: {
	// Declared value type. CLI --param strings and templated trigger params are
	// coerced to this at load time; --param-file / already-typed inputs must
	// unify with it. "list" is any JSON array; "number" allows a fractional
	// value where "int" does not.
	type:     "string" | "int" | "number" | "bool" | "list"
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
	harness?:         "claude" | "codex" | "jcode" | "fake"
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
	// "dismiss_all" | "run" | "land" | "open_pr" | "sub_workflow". Aspects only
	// weave top-level steps, not steps nested inside `when`/`repeat`.
	type?: string
	// Spawn steps only: match by role.
	role?: string
}

#Step: #SpawnStep | #WaitStep | #EvaluateStep | #DismissStep | #GateStep |
	#ReadStep | #WhenStep | #RepeatStep | #BreakStep | #StopStep |
	#ForEachStep | #WaitAllStep | #DismissAllStep | #RunStep | #LandStep |
	#OpenPrStep | #SubWorkflowStep

// Tuple categories a `read` step may match.
#Category: "fact" | "convention" | "task" | "available" | "claim" | "obstacle" |
	"need" | "artifact" | "resolution" | "event" | "message" | "suggestion" | "endorsement" | "fact_vote"

#SpawnStep: {
	type: "spawn"
	role: string | *"rat"
	// Optional reporting-boundary metadata. Workflow-owned foremen and
	// stewards are also treated as boundaries by the daemon for compatibility.
	coordination?: #Coordination
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
	// Exact review correlation, supplied by the review request rather than
	// reconstructed from the spawned reviewer's generated branch name.
	review?: {
		branch:  string
		headSha: string
		target:  string
		task:    string
		attempt: string
	}
	// Cost-tier routing predicate for THIS spawn (same semantics as a
	// `for_each` ticket's priority/labels, see #TierRule) — a single `spawn`
	// has no fanned ticket to read them from, so a workflow binds them
	// explicitly, e.g. `priority: _input.priority`.
	priority?: string
	labels?: [...string]
}

#Coordination: {
	reports_to?: "coordinator" | string
	descendant_policy?: "rollup" | "direct" | *"rollup"
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
	// Optional disjunction. `expect` is an AND over its fields, so it cannot
	// express "one success shape OR another". List alternatives here and the
	// step passes if the result unifies with `expect` OR with any entry — e.g.
	// a PR-mode land's {pr_opened: true} alongside a Direct-merge {merged: true}.
	anyOf?: [..._]
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
//
// (category, scope, identity) is NOT an identity when two instances of the same
// workflow run on one repo — bind the read to a discriminator or the newest
// match may be a concurrent peer's: `fromAgent: true` for what an agent this
// instance spawned wrote, `fromInstance: true` for what names this run.
#ReadStep: {
	type: "read"
	// Tuple category to match.
	category: #Category
	// Tuple identity to match (e.g. "review").
	identity: string
	// Scope to match; defaults to this workflow's repo name at runtime.
	scope?: string
	// Optional substring the serialized payload must contain. Mutually
	// exclusive with `fromAgent`/`fromInstance`/`forCommit`, which claim the
	// same predicate slot.
	search?: string
	// Match only the tuple THIS instance's active agent wrote — its
	// `"agent":"<name>"` stamp, bounded below by that agent's own generation,
	// exactly as `wait` bounds a `harness_result`. Use it for anything a
	// spawned agent produces for this run to route on (a review verdict), so a
	// concurrent instance's reviewer cannot satisfy the read. `rk out` stamps
	// the writer's `RK_AGENT` into every object payload that does not already
	// name an agent, so no prompt has to remember to add it. Fails CLOSED: an
	// unattributable tuple times the step out rather than routing.
	fromAgent?: bool
	// Match only the tuple whose payload names THIS workflow instance — its
	// `"instance":"<id>"` stamp, the same predicate an approval `gate` waits
	// on. Use it for anything written FOR this run rather than by an agent of
	// it: above all the `workflow_approval` event behind an approval gate,
	// where an unbound read lets two parked instances on one repo route on
	// each other's human decision (approve one, reject the other, and either
	// the rejected one merges or the approved one is held). Mutually exclusive
	// with `search`/`fromAgent`/`forCommit`. Fails CLOSED: a decision that
	// does not name this instance times the step out rather than routing.
	fromInstance?: bool
	// Match ANY tuple whose payload names this exact commit — a
	// `"head_sha":"<sha>"` substring — regardless of which agent or instance
	// wrote it. This is the commit-keyed verdict cache lookup: a review
	// artifact recorded for a branch tip is reusable by any later steward run
	// against that same unchanged tip. Unlike `fromAgent`/`fromInstance`, it
	// is deliberately NOT scoped to this run — the whole point is to find a
	// PRIOR run's verdict. Mutually exclusive with
	// `search`/`fromAgent`/`fromInstance`. The sha must be non-empty; guard
	// the step at CUE load time (an `if` over the param, not a runtime `when`)
	// when it may be absent, the same way `steward.cue` gates review tiering
	// on `diffClass`. Must be paired with `forBranch` — a sha alone is not
	// exclusive to one branch (two branches cut from the same point, before
	// either gains a new commit, share a tip).
	forCommit?: string
	// The branch `forCommit`'s sha belongs to. Required whenever `forCommit`
	// is set — the engine rejects a `forCommit` probe with no (or empty)
	// `forBranch` rather than silently running an unbound sha-only lookup.
	// Ignored when `forCommit` is unset.
	forBranch?: string
	// JSON payload field to lift (e.g. "recommendation"); whole payload if unset.
	field?: string
	// ctx variable name to store the value under (referenced by `when.var`).
	into:    string
	timeout: string | *"5m"
	// What an unmatched read does once `timeout` elapses. `"fail"` (default)
	// ends the run — the behaviour of every `read` before the commit-keyed
	// verdict cache. `"continue"` lifts `null` into `ctx.var.<into>` instead,
	// so a following `when` can route on "nothing cached yet" rather than
	// failing the instance. Meant for a short, non-blocking cache probe (a
	// small `timeout`), not as a general escape hatch for reads that name
	// something that must exist.
	onTimeout: *"fail" | "continue"
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
	// Data inputs for a repo-owned named check. The executor accepts only names
	// in the RK_CHECK_* namespace, preventing a workflow from replacing PATH,
	// loader hooks, or the supervised agent identity. Values may use ctx
	// interpolation and are passed as environment variables, never as command
	// text.
	env?: [string]: string
	// If set, the step fails the instance inline when the actual exit code
	// differs (fail-closed). If unset, the exit is only captured for a
	// following evaluate/when to route on. For a named check, overrides the
	// check's own expectExit when set.
	expectExit?: int
	// Hard wall-clock bound; a suite still running when it elapses is killed.
	// A named check's own timeout applies unless the step overrides it. What the
	// kill then does to the instance is `onTimeout`.
	timeout: string | *"10m"

	// What a blown `timeout` does to the instance.
	//   "fail"     — (default, and the only behaviour before TKT-169) the
	//                timeout is an ERROR: the step fails the instance on the
	//                spot, so a suite that is merely too slow is indistinguishable
	//                from a suite that is broken. Everything downstream — the
	//                verdict read, the routing, the operator-facing `need` — is
	//                skipped, and `rk inbox` gets a bare "timed out" failure.
	//   "continue" — the timeout is a RESULT: the killed suite reports
	//                {exit: 124, timed_out: true, verdict: "timeout"} into
	//                ctx.previousResult and the workflow keeps running, so a
	//                following `evaluate`/`when` decides what too-slow MEANS
	//                here (escalate, retry, hold the branch).
	// "continue" does NOT weaken the gate. Exit 124 is not 0, so an
	// `evaluate {expect: {exit: 0}}` or an `expectExit: 0` still rejects a
	// timed-out suite exactly like a red one — it only buys the workflow the
	// chance to say so deliberately instead of dying mid-flight.
	onTimeout: *"fail" | "continue"

	// Lift one field of this step's result into ctx.var.<into>, so a following
	// `when` can ROUTE on how the check went rather than only fail on it. The
	// same (field, into) pair a `read` step carries; the whole result object is
	// stored when `field` is unset. The result fields are:
	//   exit       process exit code (124 on a killed timeout, by the timeout(1)
	//              convention)
	//   stdout     captured stdout (empty on a timeout — the readers are killed
	//              with the child)
	//   stderr     captured stderr, or the timeout explanation on a timeout
	//   timed_out  true iff the wall-clock bound killed the command
	//   verdict    "pass" (exit 0) | "timeout" | "fail" (any other exit)
	// Route on `verdict`: it is the three-way distinction `exit` alone cannot
	// make, since a suite may legitimately exit 124 on its own.
	field?: string
	into?:  string

	// Automatic retries for a CHARACTERIZED-flaky check: on a non-"pass" verdict
	// (fail or timeout), re-run the same command up to this many additional
	// times before giving up, with a short backoff between attempts. 0 (default)
	// is off — the historical behaviour, and still the right default for a check
	// that is red because the code under test is actually broken; a retry there
	// only delays the same fail-closed outcome. Set this only on a check already
	// known to flake for reasons outside the code being checked (e.g. machine
	// load from concurrent fleet builds) — never as a first response to a single
	// red run. Every attempt's {verdict, exit} is recorded in `retries` on the
	// final result, and the durable gate-failure artifact (written on the final
	// non-"pass" verdict, if any) carries the same history, so a retried flake
	// stays visible instead of quietly disappearing on the second try.
	//
	// Bounded to <=20 (the `repeat` max cap analog): unbounded lets a
	// mis-authored value (or one interpolated from an untrusted `_input`) push
	// `retryOnFail + 1` toward u32::MAX, which panics on overflow in debug and
	// would otherwise reach the attempt loop with zero real attempts
	// (TKT-01M02QT9KTDY2CN6YJEVP3VCF8). The daemon enforces the same cap again
	// on the resolved value — this is defense-in-depth, not the only gate.
	retryOnFail: int & >=0 & <=20 | *0
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

// Open a pull/merge request for a NAMED branch against a NAMED target — the
// PR counterpart to `land`. Where `land` routes on the repo's registered merge
// mode (a Direct merge, or a PR only if the repo is registered PR-mode),
// `open_pr` ALWAYS pushes the branch and opens a pull/merge request, regardless
// of repo policy. This lets a workflow choose the review-by-PR outcome
// explicitly — e.g. `pr-on-approve.cue` gates on a human then opens a PR even in
// a repo whose default merge mode is Direct.
//
// Both fields interpolate {{ctx.*}}: `branch: "{{ctx.activeBranch}}"` opens a PR
// for the branch the workflow is holding. The branch is pushed and left standing
// (never merged, never deleted); the result in ctx.previousResult is {branch,
// target, merged: false, pr_opened, pr_url, detail}. A push/auth failure is a
// clean {pr_opened: false} (NOT an error), so gate on it with a following
// `evaluate {expect: {pr_opened: true}}` if a failed hand-off should surface.
//
// SAFETY: like `land`, `open_pr` performs no review of its own — reach it only
// through an APPROVE `when`-branch or after an approval gate.
#OpenPrStep: {
	type:   "open_pr"
	branch: string
	target: string
}

// Run another workflow as a step of this one — composition, so a macro like
// "decompose the backlog, then drain it" is one `sub_workflow` step onto the
// existing `backlog-drain` definition rather than a hand-copied duplicate of its
// steps. The named workflow is resolved and launched exactly like a top-level
// `rk workflow run <name>`: `<repo>/.rk/workflows/<name>.cue` wins over the
// global dir. It runs to completion INLINE (this step blocks on it), and its
// final result joins back into the parent's `ctx.previousResult` — so a
// following `evaluate`/`when` can gate on how the child finished, e.g.
// `evaluate {expect: {all_ok: true}}` after composing a fan-out drain.
//
// `params` are templated with the parent's `{{ctx.*}}` placeholders at run time
// (like a `run` command), then coerced to the child's declared `#Param` types —
// so forward a parent param with CUE interpolation, `params: {limit: "\(_input.limit)"}`.
// A child failure fails this step (fail-closed), surfacing in `rk inbox` as its
// own failed instance plus the parent's failure.
//
// SAFETY: nesting is bounded by a hard runtime depth cap (the depth analog of
// the `repeat` max cap) — a workflow cycle (A→B→A…) fails closed at the cap
// rather than recursing forever.
#SubWorkflowStep: {
	type: "sub_workflow"
	// Workflow definition name (or a path to a `.cue` file), resolved like
	// `rk workflow run`.
	workflow: string
	// Registered repo/path to run the child in; defaults to the parent's repo.
	repo?: string
	// Params for the child, each templated from the parent's ctx then coerced to
	// the child's declared param type. Omit for a child whose params all default.
	params?: [string]: string
}
