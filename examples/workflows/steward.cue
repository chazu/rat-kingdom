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
//   -> DIFF-SCOPE GATE (#20): refuse to auto-merge diffs over a size budget
//   -> RUN GATE  (#6):    run the repo's real test/lint suite (real teeth),
//                         routing green / red / too-slow separately (#169)
//   -> read the reviewer's APPROVE/REWORK/STOP verdict artifact
//   -> when verdict:
//        APPROVE -> land the branch straight onto main (#23) — auto-merge
//        REWORK  -> file a follow-up ticket (durable hand-off), hold the branch
//        STOP    -> escalate to the operator via a `need` tuple, hold the branch
//        (other) -> escalate + fail loudly (an unknown verdict is a bug)
//
// Every gate fails CLOSED: a protected-path violation, an over-budget diff, a
// red suite, or a suite that never finished all hold the branch unmerged and
// surface in `rk inbox`. Auto-merge is only ever reached through a clean policy
// gate, a within-budget diff, a green suite, AND an explicit APPROVE — never on
// a reviewer's word alone.
//
// This file is the SOURCE for every deployed copy. Install it with
// `rk workflow install examples/workflows/steward.cue` rather than `cp`, and the
// install manifest lets `rk workflow drift` tell you when a deployed steward has
// been hand-edited away from this definition or left behind by it (TKT-176) —
// which is how the live `steward-grmpl` kept a 30m gate and an unbound verdict
// read that no repo source could fix.
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
		// The repo's real NAMED check — the run gate's teeth (#6). The command
		// lives in `.rk/checks.cue`, not in this workflow definition, so the
		// steward remains compatible with `require_named_checks = true`.
		check: {type: "string", required: false, default: "verify"}
		// POLICY GUARDRAIL (#19): an ERE matched against the names of files the
		// branch changes vs `target`. A hit means the diff touches a protected
		// path, so the steward refuses to auto-merge and escalates to a human.
		// Tune per repo; the default guards CI, reactor, and migration surfaces.
		protectedPaths: {type: "string", required: false, default: "(^|/)(\\.github|\\.rk|migrations)/"}
		// DIFF-SCOPE GUARDRAIL (#20): a per-repo size budget on the branch's diff
		// vs `target`. A branch that changes MORE than maxDiffFiles files OR adds+
		// removes MORE than maxDiffLines lines is too big to auto-merge on a cheap
		// reviewer's word — the steward holds it for a human (surfaced in rk inbox)
		// instead of APPROVE. This bounds the blast radius of a runaway rat that
		// dodges protected paths but rewrites half the repo. 0 disables a budget;
		// tune per repo. A hold is not a reject: the operator merges by hand.
		maxDiffFiles: {type: "int", required: false, default: 50}
		maxDiffLines: {type: "int", required: false, default: 2000}
		reviewTimeout: {type: "string", required: false, default: "15m"}
		// RUN-GATE BUDGET (TKT-169). The steward re-runs `check` in the
		// reviewer's OWN worktree, which is a cold checkout: no warm build
		// cache, and several stewards fired by the reactor competing for the
		// same cores. So the honest budget is not the suite's warm wall-clock —
		// it is that number with room for a full rebuild under contention.
		// The live grmpl steward ran a 10-15m suite on a 30m bound and blew it,
		// failing instances whose review had already passed; 60m is the same
		// suite with the cold-cache headroom it actually needs.
		//
		// Tune this per repo rather than editing the step, and prefer SCOPING
		// the gate over raising the bound forever: point `check` at a named
		// entry in `<repo>/.rk/checks.cue` (which carries its own timeout) so
		// the repo owns both the command and its budget.
		gateTimeout: {type: "string", required: false, default: "60m"}
	}

	agents: {
		// Steward reviews follow the fleet's configured Codex/Luna execution
		// policy instead of silently routing this one workflow through Claude.
		default:  {harness: "codex", model: "gpt-5.6-luna"}
		reviewer: {harness: "codex", model: "gpt-5.6-luna"}
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
			check: "steward-protected-paths"
			env: {
				RK_CHECK_TARGET:          _input.target
				RK_CHECK_PROTECTED_PATHS: _input.protectedPaths
			}
			timeout: "2m"
		},
		{type: "evaluate", expect: {exit: 0}},

		// 2b. DIFF-SCOPE GATE (#20). Count the files the branch changes and the
		//     added+removed lines vs the merge target, and exit non-zero if EITHER
		//     exceeds its per-repo budget (0 = that budget off). Binary files (a
		//     `-` in --numstat) count as 0 lines. The following evaluate turns an
		//     over-budget diff into a fail-closed hold — a sprawling branch goes
		//     to the operator (rk inbox), never to auto-merge, no matter how clean
		//     its verdict.
		{
			type: "run"
			check: "steward-diff-scope"
			env: {
				RK_CHECK_TARGET:         _input.target
				RK_CHECK_MAX_DIFF_FILES: "\(_input.maxDiffFiles)"
				RK_CHECK_MAX_DIFF_LINES: "\(_input.maxDiffLines)"
			}
			timeout: "2m"
		},
		{type: "evaluate", expect: {exit: 0}},

		// 3. RUN GATE (#6). The repo's real suite, executed in the reviewer's
		//    worktree — a verdict the harness cannot forge. Nothing below this
		//    reaches `land` unless the suite came back green.
		//
		//    Unlike the two gates above, this one routes on THREE outcomes
		//    instead of two (TKT-169). `evaluate {exit: 0}` collapses "the suite
		//    says no" and "the suite did not finish inside \(_input.gateTimeout)"
		//    into one bare instance failure, and the second is the common one:
		//    the steward's re-run is a cold worktree competing with its peers,
		//    so a suite that is merely slow killed runs whose review had already
		//    passed, with nothing in `rk inbox` to say why. `onTimeout:
		//    "continue"` turns the blown budget into a result, and `verdict`
		//    lifts the three-way answer for the `when` below.
		//
		//    Still fail-closed, in both directions: a timeout reports exit 124,
		//    so it is no more mergeable than a red suite. It just gets a hand-off
		//    that names the real problem.
		{
			type:      "run"
			check:     _input.check
			timeout:   _input.gateTimeout
			onTimeout: "continue"
			field:     "verdict"
			into:      "gate"
		},
		{
			type: "when"
			var:  "gate"
			cases: {
				// GREEN. Fall through to the verdict read and the routing below
				// — the only path that can reach `land`.
				"pass": []

				// TOO SLOW. An infrastructure condition, not a bad branch: the
				// suite never got to say anything. Escalate with the budget in
				// the text (so the operator can raise `gateTimeout` or scope
				// `check` without digging), HOLD the branch, and END the run
				// here — `break` at top level finishes the instance cleanly.
				//
				// The `break` is load-bearing. Without it the `when` falls
				// through to the verdict read and the APPROVE arm would LAND a
				// branch whose suite never completed.
				//
				// Clean completion, not `stop`: a timeout is a capacity signal
				// the operator tunes, and failing the instance would add a
				// second red mark in `rk inbox` for the one event the `need`
				// already reports.
				"timeout": [
					{
						type: "run"
						check: "steward-report-timeout"
						env: {
							RK_CHECK_REPO:         _input.repo
							RK_CHECK_TASK_ID:      _input.taskId
							RK_CHECK_GATE_TIMEOUT: _input.gateTimeout
							RK_CHECK_BRANCH:       "{{ctx.activeBranch}}"
						}
						timeout: "2m"
					},
					{type: "dismiss", noMerge: true},
					{type: "break"},
				]
			}
			// RED (or a check that could not run at all). The branch is broken,
			// which IS a failure — escalate, hold it, and fail loudly, the same
			// shape as an unrecognized verdict below.
			default: [
				{
					type: "run"
					check: "steward-report-gate-failure"
					env: {
						RK_CHECK_REPO:    _input.repo
						RK_CHECK_TASK_ID: _input.taskId
						RK_CHECK_BRANCH:  "{{ctx.activeBranch}}"
					}
					timeout: "2m"
				},
				{type: "dismiss", noMerge: true},
				{type: "stop", reason: "run gate failed for " + _input.taskId},
			]
		},
		// Keep the run gate's exit assertion explicit as a final fail-closed
		// guard. The timeout and red branches terminate above; only the green
		// branch reaches this assertion and then the review verdict.
		{type: "evaluate", expect: {exit: 0}},

		// 4. Lift the reviewer's verdict into ctx.var.verdict.
		//
		//    `fromAgent` is load-bearing, not decoration (TKT-161). The steward
		//    is fired PER rat completion, so several instances run against one
		//    repo at once by design — and they all read
		//    (artifact, <repo>, review). Without the binding "newest wins"
		//    hands this steward whichever reviewer finished last, which may be
		//    a peer instance's, and the APPROVE below lands a branch on a
		//    stranger's verdict. Bound, the read matches only the tuple the
		//    reviewer spawned in step 1 wrote (`rk out` stamps every payload
		//    with its writer's name), and fails CLOSED into `rk inbox` if that
		//    reviewer recorded nothing — never onto somebody else's word.
		{
			type:      "read"
			category:  "artifact"
			identity:  "review"
			fromAgent: true
			field:     "recommendation"
			into:      "verdict"
			timeout:   "5m"
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
					// GATE THE POLICY RESULT. Every repository delivery mode reports
					// the same `delivered` truth: local merge, merge+push, branch push,
					// or PR/MR hand-off. A conflict or push failure stays false and
					// holds the branch for rk inbox.
					{type: "evaluate", expect: {delivered: true}},
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
						check: "steward-file-rework-ticket"
						env: {
							RK_CHECK_REPO:    _input.repo
							RK_CHECK_TASK_ID: _input.taskId
							RK_CHECK_BRANCH:  "{{ctx.activeBranch}}"
						}
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
						check: "steward-report-stop"
						env: {
							RK_CHECK_REPO:    _input.repo
							RK_CHECK_TASK_ID: _input.taskId
							RK_CHECK_BRANCH:  "{{ctx.activeBranch}}"
						}
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
					check: "steward-report-unknown-verdict"
					env: {
						RK_CHECK_REPO:    _input.repo
						RK_CHECK_TASK_ID: _input.taskId
						RK_CHECK_BRANCH:  "{{ctx.activeBranch}}"
					}
					timeout: "2m"
				},
				{type: "dismiss", noMerge: true},
				{type: "stop", reason: "unrecognized review verdict for " + _input.taskId},
			]
		},
	]
}
