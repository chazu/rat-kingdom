// steward: reactive, unattended triage of a completed rat's branch — the
// biggest single reduction in per-task operator attention. It automates the
// most-repeated operator decision, "is this branch good to merge?", and asks a
// human ONLY for the genuine judgment calls.
//
// This is not run by hand; it is FIRED by the reactor on every rat completion.
// Register the `steward-on-completion` trigger (examples/triggers.cue) — it
// matches `Event/harness_result` with `"role":"rat"` and passes the completed
// rat's {task, branch, repo} through. The `"role":"rat"` scope is also what
// breaks re-entrancy: the reviewer this workflow spawns completes as a
// "reviewer", so its own harness_result never re-fires the steward.
//
// Flow (all steps already exist — steward is their reactive application):
//   spawn a cheap reviewer chained onto the completed branch
//   -> POLICY GATE (#19): refuse to auto-merge diffs touching protected paths
//   -> RUN GATE  (#6):    run the repo's real test/lint suite (real teeth)
//   -> read the reviewer's APPROVE/REWORK/STOP verdict artifact
//   -> when verdict:
//        APPROVE -> land the branch straight onto main (#23) — auto-merge
//        REWORK  -> file a follow-up ticket (durable hand-off), hold the branch
//        STOP    -> escalate to the operator via a `need` tuple, hold the branch
//        (other) -> escalate + fail loudly (an unknown verdict is a bug)
//
// Both gates fail CLOSED: a protected-path violation or a red suite fails the
// instance, so the branch is never merged and the failure surfaces in
// `rk inbox`. Auto-merge is only ever reached through a clean policy gate, a
// green suite, AND an explicit APPROVE — never on a reviewer's word alone.
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/, and copy
// the matching trigger into ~/.rat-kingdom/triggers/ (or <repo>/.rk/).
workflow: {
	name:        "steward"
	description: "reactive triage of a completed branch: policy+test gate, verdict -> auto-merge / rework-ticket / escalate"

	params: {
		// Identifies the completed work in ticket/need text. String-interpolated
		// by the trigger from the completion tuple, so always a string.
		taskId: {type: "string", required: false, default: "unknown"}
		// The completed rat's branch — the reviewer chains onto it. Passed
		// through raw by the trigger; empty only for branchless (attach) rats,
		// where the reviewer spawn fails closed (surfaces in `rk inbox`).
		branch: {type: "string", required: false, default: ""}
		// The repo the completion is scoped to. Needed to file the rework ticket
		// and the escalation need in the right scope.
		repo: {type: "string", required: false, default: "rat-kingdom"}
		// Where an approved branch lands. The reviewer's own base is the work
		// branch, so only an explicit `land` can put it on main.
		target: {type: "string", required: false, default: "main"}
		// The repo's real check command — the run gate's teeth (#6). Run
		// verbatim in the reviewer's worktree (checked out on the branch).
		check: {type: "string", required: false, default: "cargo test --quiet"}
		// POLICY GUARDRAIL (#19): an ERE matched against the names of files the
		// branch changes vs `target`. A hit means the diff touches a protected
		// path, so the steward refuses to auto-merge and escalates to a human.
		// Tune per repo; the default guards CI, reactor, and migration surfaces.
		protectedPaths: {type: "string", required: false, default: "(^|/)(\\.github|\\.rk|migrations)/"}
		reviewTimeout: {type: "string", required: false, default: "15m"}
		gateTimeout: {type: "string", required: false, default: "20m"}
	}

	agents: {
		// Review runs on the cheap model: the steward's whole point is that the
		// common (clean) case costs almost nothing.
		default:  {harness: "claude", model: "haiku"}
		reviewer: {harness: "claude", model: "haiku"}
	}

	steps: [
		// 1. A cheap reviewer chains onto the completed branch (spawn.branch is
		//    the base), so its worktree carries the completed work and every
		//    following `run` executes against it.
		{
			type:   "spawn"
			role:   "reviewer"
			agent:  "reviewer"
			branch: _input.branch
			task: {
				title: "steward-review-" + _input.taskId
				description: """
					You are the steward's reviewer on branch {{ctx.activeBranch}},
					chained off a rat's completed work for: \(_input.taskId)

					Compare with: git log \(_input.target)..HEAD and git diff \(_input.target)...HEAD

					Decide APPROVE (clean, safe to auto-merge), REWORK (fixable issues
					remain), or STOP (fundamentally wrong / needs a human call). Record
					the verdict before finishing so the steward can route on it:
					rk out artifact \(_input.repo) review --payload '{"task": "\(_input.taskId)", "recommendation": "APPROVE|REWORK|STOP", "notes": "..."}'
					"""
			}
		},
		{type: "wait", timeout: _input.reviewTimeout},
		// The reviewer session must itself have finished cleanly.
		{type: "evaluate", expect: {is_error: false}},

		// 2. POLICY GATE (#19). List the files the branch changes vs the merge
		//    target; if ANY match the protected pattern, exit non-zero. The
		//    following evaluate turns that into a fail-closed hold — protected
		//    diffs go to the operator, never to auto-merge.
		{
			type: "run"
			command: "! git diff --name-only \(_input.target)...HEAD | grep -qE '\(_input.protectedPaths)'"
			timeout: "2m"
		},
		{type: "evaluate", expect: {exit: 0}},

		// 3. RUN GATE (#6). The repo's real suite, executed in the reviewer's
		//    worktree — a verdict {exit,stdout,stderr} the harness cannot forge.
		//    A red suite fails the instance closed: the branch is never merged.
		{type: "run", command: _input.check, timeout: _input.gateTimeout},
		{type: "evaluate", expect: {exit: 0}},

		// 4. Lift the reviewer's verdict into ctx.var.verdict.
		{
			type:     "read"
			category: "artifact"
			identity: "review"
			field:    "recommendation"
			into:     "verdict"
			timeout:  "5m"
		},

		// 5. Route on the verdict. Gates already passed, so APPROVE is the ONLY
		//    path that reaches `land`; everything else holds the branch unmerged.
		{
			type: "when"
			var:  "verdict"
			cases: {
				// AUTO-MERGE: tear down the reviewer's worktree (so its branch is
				// no longer checked out), then land that branch — carrying the
				// reviewed work — straight onto the target. The run completes.
				"APPROVE": [
					{type: "dismiss", noMerge: true},
					{type: "land", branch: "{{ctx.activeBranch}}", target: _input.target},
					// GATE THE LAND RESULT. `land` routes on the repo's merge mode:
					// a Direct-merge repo reports {merged: true}; a PR-mode repo
					// pushes the branch and opens a PR, reporting {pr_opened: true}
					// (never merged) — surfaced to the operator as an awaiting-review
					// row in rk inbox. BOTH are a clean hand-off, so accept either.
					// Only a conflict / moved target / push failure (merged:false AND
					// pr_opened:false) fails closed, holding the branch for rk inbox.
					{type: "evaluate", expect: {merged: true}, anyOf: [{pr_opened: true}]},
				]
				// REWORK: hand the fixable work back durably as a ticket rather
				// than looping a rework rat here — the steward stays fast and
				// single-purpose, and the backlog drain / dispatcher picks it up
				// (whose completion re-enters the steward: a closed loop, not a
				// runaway). The branch is HELD (noMerge) so the rework can build
				// on it. `rk ticket new` runs in the reviewer's worktree.
				"REWORK": [
					{
						type: "run"
						command: "rk ticket new 'rework: \(_input.taskId)' --repo \(_input.repo) --body 'Steward routed REWORK on branch {{ctx.activeBranch}}. Read the reviewer notes: rk scan artifact \(_input.repo)'"
						timeout: "2m"
					},
					{type: "dismiss", noMerge: true},
				]
				// ESCALATE: a legitimate human judgment call. Emit a `need` in the
				// repo scope — `rk inbox` (#12) ranks it into the operator's queue
				// — and HOLD the branch unmerged for inspection. Clean completion:
				// a STOP is a normal outcome, not a failure.
				"STOP": [
					{
						type: "run"
						command: "rk out need \(_input.repo) steward --payload '{\"agent\":\"steward\",\"task\":\"\(_input.taskId)\",\"text\":\"steward: reviewer returned STOP for \(_input.taskId) on {{ctx.activeBranch}} — needs a human merge decision; branch held unmerged\"}'"
						timeout: "2m"
					},
					{type: "dismiss", noMerge: true},
				]
			}
			// An unrecognized verdict is a bug, not a veto: escalate AND fail the
			// instance loudly (branch held) rather than silently merging.
			default: [
				{
					type: "run"
					command: "rk out need \(_input.repo) steward --payload '{\"agent\":\"steward\",\"task\":\"\(_input.taskId)\",\"text\":\"steward: unrecognized review verdict for \(_input.taskId) on {{ctx.activeBranch}} — branch held unmerged, needs a human\"}'"
					timeout: "2m"
				},
				{type: "dismiss", noMerge: true},
				{type: "stop", reason: "unrecognized review verdict for " + _input.taskId},
			]
		},
	]
}
