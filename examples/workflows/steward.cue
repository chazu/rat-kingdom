// STATUS (Phase 4 of the steward remediation, TKT-01M036PSF2WV7NHZE00G2EFCVK):
// NO LONGER LIVE. The operator cut over to the daemon-native landing pipeline
// on 2026-08-16 (docs/proposals/daemon-native-landing-pipeline.md §6) — the
// live `steward-landing-on-completion` trigger (action: land) replaced
// `steward-on-completion`, so nothing fires this workflow by name anymore.
// Its gate/routing logic (`_gates`, `_routeVerdict`, tiering) is superseded by
// `crates/rk-daemon/src/landing.rs`; its one still-needed piece — spawning a
// reviewer and waiting for it — lives on as `examples/workflows/steward-review.cue`,
// invoked programmatically by `LandingPipeline::request_review`. This file
// remains only as the pre-cutover reference implementation, load-bearing for
// its own schema/routing tests (`crates/rk-workflow/tests/examples.rs`,
// `crates/rk-daemon/tests/workflow_verdict_cache.rs`) and
// `examples/triggers.cue`'s `steward-on-completion` entry. Deleting all of
// that together is tracked separately: TKT-01M048ASYM00N37EBK1VM7FH5H,
// "Remove obsolete steward CUE gate definitions after pipeline cutover".
//
// steward: reactive, unattended triage of a completed rat's branch — the
// biggest single reduction in per-task operator attention. It automates the
// most-repeated operator decision, "is this branch good to merge?", and asks a
// human ONLY for the genuine judgment calls.
//
// This is not run by hand; it was FIRED by the reactor on every rat completion
// before the cutover above.
// Register the `steward-on-completion` trigger (examples/triggers.cue) — it
// matches `Event/harness_result` with `"role":"rat"` and passes the completed
// rat's {task, branch, repo, diffClass, headSha} through. The `"role":"rat"`
// scope is also what breaks re-entrancy: both tiers below spawn as role
// "reviewer", so neither one's own harness_result ever re-fires the steward on
// the branch it just handled.
//
// REVIEW TIERING (TKT-01M036N1RT74H6NPRH5FMM8A6T). `diffClass` — a
// precomputed classification the daemon stamps on every completion payload —
// picks one of two tiers at CUE LOAD TIME (a `list.Concat`/`if` selection over
// `_input.diffClass`, not a runtime `when` step: workflow params are never
// copied into ctx.vars, so a runtime `when` cannot see `_input` at all):
//
//   REVIEW TIER — diffClass "small"/"large" (and any legacy completion missing
//   the field, which defaults here to "large", fail-closed toward review):
//     spawn a cheap reviewer chained onto the completed branch
//     -> POLICY GATE (#19): refuse to auto-merge diffs touching protected paths
//     -> DIFF-SCOPE GATE (#20): refuse to auto-merge diffs over a size budget
//     -> RUN GATE  (#6):    run the repo's real test/lint suite (real teeth),
//                           routing green / red / too-slow separately (#169)
//     -> read the reviewer's APPROVE/REWORK/STOP verdict artifact
//     -> when verdict:
//          APPROVE -> land the branch straight onto main (#23) — auto-merge
//          REWORK  -> file a follow-up ticket (durable hand-off), hold branch
//          STOP    -> escalate to the operator via a `need` tuple, hold branch
//          (other) -> escalate + fail loudly (an unknown verdict is a bug)
//
//   REDUCED TIER — diffClass "doc-only"/"trivial" (TKT-01M0382WS1NNR0W5RA59PPDDYP):
//     the LLM review turn ($3-8) is skipped, but the gates are NOT — a
//     workflow `run` step (what runs the protected-paths/diff-scope/verify
//     gates) only ever executes in an ALREADY-spawned agent's worktree
//     (`ctx.active_agent`, set only by `spawn`); there is no primitive to
//     adopt an existing agent's worktree instead. So this tier still spawns —
//     a near-zero GATE-HOLDER, on the cheapest configured model, whose entire
//     task is "call `rk done` immediately". Its worktree then hosts the exact
//     same POLICY/DIFF-SCOPE/RUN gates as the review tier, and once they pass
//     it lands UNCONDITIONALLY — there is no verdict to route on, because
//     nothing looked at the diff. Net effect: the $3-8 reasoning turn is
//     skipped for diffs too small to need it; a ~cent agent shell remains
//     live until TKT-01M036PSEHTMD3S5D2JFAG7XVY's daemon-native landing
//     pipeline removes the worktree coupling entirely (tracked there, not
//     solved here).
//
// Every gate fails CLOSED in BOTH tiers: a protected-path violation, an
// over-budget diff, a red suite, or a suite that never finished all hold the
// branch unmerged and surface in `rk inbox`. Tiering only ever decides whether
// an LLM looks at the diff before the gates run — it never weakens a gate, and
// auto-merge is only ever reached through a clean policy gate, a within-budget
// diff, and a green suite (plus an explicit APPROVE in the review tier).
//
// COMMIT-KEYED VERDICT CACHE (Phase 2, TKT-01M036NWEG0H019BJ16G59RZVP, and its
// rework), review tier only. A retry against an UNCHANGED branch tip — the
// reactor refiring, an operator re-running steward by hand after clearing an
// infra hold, a daemon restart replaying a completion — used to re-spawn and
// re-pay for a reviewer even though nothing about the diff had changed.
// Before spawning one, the workflow now probes (bounded, non-blocking:
// `forCommit`+`forBranch`, `onTimeout: "continue"`) whether ANY prior run
// already recorded a verdict for this EXACT `headSha` ON THIS EXACT `branch`.
// Both are bound, not just the sha (the rework's fix): two branches cut from
// the same point, before either gains a new commit, share a tip commit, and a
// sha-only probe would let branch A's verdict — reviewing branch A's diff
// against `target` — satisfy a lookup for branch B, whose diff against
// `target` may be completely different despite the shared HEAD.
//
// On a hit it does NOT spawn a reviewer — that would let a retry shop for a
// better opinion, defeating the cache. It spawns only a cheap gate-holder
// (same shape as the reduced tier) to re-verify the deterministic gates
// against `target`'s current state, then routes on the cached recommendation
// through the same `_routeVerdict` logic a fresh review uses. A cached
// REWORK/STOP is honored exactly like a fresh one. On a miss (nothing cached,
// or no `headSha`/`branch` to key on at all) it falls straight through to the
// unchanged review tier below. Cache invalidation is automatic: a new commit
// is a new sha, so a real code change is never served a stale verdict.
//
// DECIDED (operator call, rework of TKT-01M036NWEG0H019BJ16G59RZVP): the
// cache-hit arm below still spawns and waits for a gate-holder — it is NOT a
// shortcut that skips straight to routing the cached verdict. This is
// intentional, not an oversight: "skip the reviewer" means skip the expensive
// LLM judgment turn, not skip verification entirely. The deterministic gates
// (protected paths, diff-scope, the real test suite) still run on every
// landing attempt, cached verdict or not, because a suite that was green
// yesterday can be red today (a flake, a dependency bump, `target` moving
// under it) and because a `run`/`land` step needs SOME agent's worktree to
// execute in regardless (there is no primitive, pre-Phase-3, to run a gate
// without an active spawn). Only the $3-8 reasoning turn is skipped; nothing
// downstream of it is weakened.
//
// This file is the SOURCE for every deployed copy. Install it with
// `rk workflow install examples/workflows/steward.cue` rather than `cp`, and the
// install manifest lets `rk workflow drift` tell you when a deployed steward has
// been hand-edited away from this definition or left behind by it (TKT-176) —
// which is how the live `steward-grmpl` kept a 30m gate and an unbound verdict
// read that no repo source could fix.
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/, and copy
// the matching trigger into ~/.rat-kingdom/triggers/ (or <repo>/.rk/). BOTH
// installed copies need an operator sync after this file changes — the
// diffClass/headSha templating landed in the repo source, not the deployed one.
package workflow

import "list"

workflow: {
	name:        "steward"
	description: "reactive triage of a completed branch: policy+test gate, verdict -> auto-merge / rework-ticket / escalate"

	params: {
		// Identifies the completed work in ticket/need text. String-interpolated
		// by the trigger from the completion tuple, so always a string.
		taskId: {type: "string", required: false, default: "unknown"}
		// The completed rat's branch — the reviewer (or gate-holder) chains onto
		// it. Passed through raw by the trigger; empty only for branchless
		// (attach) rats, where the spawn fails closed (surfaces in `rk inbox`).
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
		// INVARIANT (order-your-timers-below-workflow-waits): must stay
		// comfortably ABOVE the daemon's `supervisor.stuck_after_secs` (default
		// 10m, crates/rk-core/src/config.rs — see the doc comment on
		// `SupervisorConfig::stuck_after_secs`, which cross-references this
		// field). If a wait here times out at or before the sweep flags the
		// reviewer as stuck, the review hard-fails before the sweep's soft
		// steer ("still working? wrap up") ever gets a chance to nudge it back
		// to a clean `rk done` — the sweep and this wait raced by coincidence
		// once when both were 15m. `SupervisorConfig::review_timeout_warning`
		// checks this pairing (against this shipped default) at daemon
		// startup; re-check it by hand if you tune this value per repo.
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
		// REVIEW TIERING (see file header). The completion payload's precomputed
		// diff classification — "doc-only" | "trivial" | "small" | "large".
		// "doc-only"/"trivial" take the REDUCED tier (gate-holder, no LLM
		// review); everything else — including a legacy completion missing the
		// field, since the reactor omits a null-templated param rather than
		// passing it through so this default applies (TKT-146/f359a95) — takes
		// the unchanged review tier. Default "large" is deliberately fail-closed
		// toward review.
		diffClass: {type: "string", required: false, default: "large"}
		// The completed rat's branch-tip SHA, templated by the trigger from the
		// completion payload. Threaded into the reviewer's task and its verdict
		// artifact for future commit-keyed verdict caching (Phase 2); empty for
		// a legacy completion.
		headSha: {type: "string", required: false, default: ""}
	}

	agents: {
		// Steward reviews follow the fleet's configured Codex/Luna execution
		// policy instead of silently routing this one workflow through Claude.
		default:  {harness: "codex", model: "gpt-5.6-luna"}
		reviewer: {harness: "codex", model: "gpt-5.6-luna"}
		// REDUCED TIER gate-holder (see file header): the cheapest configured
		// model, since this agent does no reasoning over the diff — its whole
		// job is to call `rk done` and host the deterministic gates that follow.
		gateholder: {harness: "claude", model: "haiku"}
	}

	// Shared by BOTH tiers: POLICY GATE (#19), DIFF-SCOPE GATE (#20), and the
	// RUN GATE (#6) with its three-way (pass/timeout/red) routing (TKT-169) and
	// characterized-flake retry (TKT-01M02AMKD24WZVVMARJPXKYKSW). Tiering never
	// weakens these — it only ever decides whether an LLM looks at the diff
	// BEFORE they run.
	_gates: [
		// POLICY GATE (#19). List the files the branch changes vs the merge
		// target; if ANY match the protected pattern, exit non-zero. The
		// following evaluate turns that into a fail-closed hold — protected
		// diffs go to the operator, never to auto-merge.
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

		// DIFF-SCOPE GATE (#20). Count the files the branch changes and the
		// added+removed lines vs the merge target, and exit non-zero if EITHER
		// exceeds its per-repo budget (0 = that budget off). Binary files (a
		// `-` in --numstat) count as 0 lines. The following evaluate turns an
		// over-budget diff into a fail-closed hold — a sprawling branch goes
		// to the operator (rk inbox), never to auto-merge, no matter how clean
		// its verdict.
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

		// RUN GATE (#6). The repo's real suite, executed in the active agent's
		// worktree — a verdict the harness cannot forge. Nothing below this
		// reaches `land` unless the suite came back green.
		//
		// Unlike the two gates above, this one routes on THREE outcomes
		// instead of two (TKT-169). `evaluate {exit: 0}` collapses "the suite
		// says no" and "the suite did not finish inside \(_input.gateTimeout)"
		// into one bare instance failure, and the second is the common one:
		// the steward's re-run is a cold worktree competing with its peers,
		// so a suite that is merely slow killed runs whose review had already
		// passed, with nothing in `rk inbox` to say why. `onTimeout:
		// "continue"` turns the blown budget into a result, and `verdict`
		// lifts the three-way answer for the `when` below.
		//
		// Still fail-closed, in both directions: a timeout reports exit 124,
		// so it is no more mergeable than a red suite. It just gets a hand-off
		// that names the real problem.
		//
		// retryOnFail: 1 (TKT-01M02AMKD24WZVVMARJPXKYKSW). This specific gate
		// is CHARACTERIZED flaky: several stewards fire at once, each running
		// a full cold-worktree `mise run verify`, and the fleet machine has
		// been observed at a load average an order of magnitude over its core
		// count while that happens — timing-sensitive daemon tests miss their
		// budget under that contention for reasons having nothing to do with
		// the branch under review. One extra attempt (after a short backoff)
		// rides out that transient without weakening the gate: a genuinely
		// red suite fails the retry too and reaches the same `default` arm
		// below, just a few seconds later. Every attempt is recorded — on
		// `gate.retries` when it recovers, and in the durable
		// `(artifact, <repo>, gate-failure)` tuple the failed final attempt
		// writes below — so a retried flake stays visible instead of quietly
		// looking like a clean first try.
		{
			type:        "run"
			check:       _input.check
			timeout:     _input.gateTimeout
			onTimeout:   "continue"
			retryOnFail: 1
			field:       "verdict"
			into:        "gate"
		},
		{
			type: "when"
			var:  "gate"
			cases: {
				// GREEN. Fall through to whatever follows the gates in this
				// tier — the only path that can reach `land`.
				"pass": []

				// TOO SLOW. An infrastructure condition, not a bad branch: the
				// suite never got to say anything. Escalate with the budget in
				// the text (so the operator can raise `gateTimeout` or scope
				// `check` without digging), HOLD the branch, and END the run
				// here — `break` at top level finishes the instance cleanly.
				//
				// The `break` is load-bearing. Without it the `when` falls
				// through to whatever follows and could land a branch whose
				// suite never completed.
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
		// branch reaches this assertion and whatever follows the gates.
		{type: "evaluate", expect: {exit: 0}},
	]

	// REVIEW TIER (see file header): unchanged from the original steward — a
	// cheap reviewer's verdict gates the merge decision.
	_reviewArm: list.Concat([
		[
			// A cheap reviewer chains onto the completed branch (spawn.branch is
			// the base), so its worktree carries the completed work and every
			// following `run` executes against it.
			{
				type:   "spawn"
				role:   "reviewer"
				agent:  "reviewer"
				branch: _input.branch
				task: {
					title: "steward-review-" + _input.taskId
					description: """
						You are the steward's reviewer on branch {{ctx.activeBranch}}
						(head \(_input.headSha)), chained off a rat's completed work for:
						\(_input.taskId)

						Compare with: git log \(_input.target)..HEAD and git diff \(_input.target)...HEAD

						Decide APPROVE (clean, safe to auto-merge), REWORK (fixable issues
						remain), or STOP (fundamentally wrong / needs a human call). Record
						the verdict before finishing so the steward can route on it:
						rk out artifact \(_input.repo) review --payload '{"task": "\(_input.taskId)", "recommendation": "APPROVE|REWORK|STOP", "notes": "...", "head_sha": "\(_input.headSha)", "branch": "\(_input.branch)"}'
						"""
				}
			},
			{type: "wait", timeout: _input.reviewTimeout},
			// The reviewer session must itself have finished cleanly.
			{type: "evaluate", expect: {is_error: false}},
		],
		_gates,
		[
			// Lift the reviewer's verdict into ctx.var.verdict.
			//
			// `fromAgent` is load-bearing, not decoration (TKT-161). The steward
			// is fired PER rat completion, so several instances run against one
			// repo at once by design — and they all read
			// (artifact, <repo>, review). Without the binding "newest wins"
			// hands this steward whichever reviewer finished last, which may be
			// a peer instance's, and the APPROVE below lands a branch on a
			// stranger's verdict. Bound, the read matches only the tuple the
			// reviewer spawned above wrote (`rk out` stamps every payload with
			// its writer's name), and fails CLOSED into `rk inbox` if that
			// reviewer recorded nothing — never onto somebody else's word.
			{
				type:      "read"
				category:  "artifact"
				identity:  "review"
				fromAgent: true
				field:     "recommendation"
				into:      "verdict"
				timeout:   "5m"
			},
		],
		(_routeVerdict & {Var: "verdict"}).steps,
	])

	// Route on a verdict already sitting in ctx.var.<Var> — shared by the fresh
	// review above (`verdict`, read from the reviewer THIS run spawned) and the
	// commit-keyed cache below (`cachedVerdict`, read from ANY prior run's
	// artifact for this exact head_sha). Gates already passed either way, so
	// APPROVE is the ONLY path that reaches `land`; everything else holds the
	// branch unmerged. Parametrized so a cached REWORK/STOP is routed through
	// the IDENTICAL steps a fresh one would take — never a second, looser path.
	_routeVerdict: {
		Var: string
		steps: [
			{
				type: "when"
				var:  Var
				cases: {
					// AUTO-MERGE: tear down the reviewing/gate-holding worktree (so
					// its branch is no longer checked out), then land that branch —
					// carrying the reviewed work — straight onto the target.
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
					// on it. `rk ticket new` runs in the reviewing/gate-holding worktree.
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

	// PHASE 2 (commit-keyed verdict cache, TKT-01M036NWEG0H019BJ16G59RZVP, and
	// its rework): before paying for a reviewer, ask whether ANY prior run
	// already recorded a verdict for this EXACT branch tip — a bounded,
	// non-blocking probe (`forCommit`+`forBranch`, `onTimeout: "continue"`),
	// not a wait for one to arrive.
	//
	//   HIT  (any recommendation, including REWORK/STOP): do NOT spawn a
	//        reviewer to shop for a second opinion — that would defeat the
	//        cache and let a retry launder a bad verdict into a better one.
	//        SPAWN AND WAIT FOR A GATE-HOLDER regardless (same shape as the
	//        reduced tier) — this is not a shortcut left unfinished, it is
	//        the decided shape: a cache hit reuses the LLM's JUDGMENT, not
	//        the deterministic gates. `target` may have moved since the
	//        cached verdict was recorded, and a `run`/`land` step needs an
	//        active agent's worktree to execute in regardless (no primitive,
	//        pre-Phase-3, runs a gate without one). Then route on the cached
	//        recommendation through the SAME `_routeVerdict` a fresh review
	//        uses. No gate is weakened; only the LLM opinion is reused.
	//   MISS (`cachedVerdict` lifts as "" — `value_as_key` renders `null` that
	//        way): fall through to the unchanged `_reviewArm` below.
	//
	// Only reachable when `_input.headSha` AND `_input.branch` are both
	// non-empty (guarded where this is selected into `steps`, below) — an
	// empty sha would key on `"head_sha":""`, matching any OTHER legacy
	// artifact missing the field, which is exactly the false cache hit the
	// engine's `forCommit` guard refuses; an empty branch is refused the same
	// way by the engine's `forBranch` guard. Cache invalidation is automatic:
	// a new commit is a new sha, so there is nothing to evict by hand.
	_cachedReviewArm: [
		{
			type:      "read"
			category:  "artifact"
			identity:  "review"
			forCommit: _input.headSha
			forBranch: _input.branch
			field:     "recommendation"
			into:      "cachedVerdict"
			timeout:   "1s"
			onTimeout: "continue"
		},
		{
			type: "when"
			var:  "cachedVerdict"
			cases: {
				"": _reviewArm
			}
			default: list.Concat([
				[
					{
						type:   "spawn"
						role:   "reviewer"
						agent:  "gateholder"
						branch: _input.branch
						task: {
							title: "steward-gatehold-cached-" + _input.taskId
							description: """
								You are a gate-holder, not a reviewer. A prior review
								already recorded a verdict for branch
								{{ctx.activeBranch}} at this EXACT commit (head
								\(_input.headSha)) for \(_input.taskId), so the steward
								is reusing that cached verdict instead of paying for a
								fresh review (commit-keyed verdict cache, Phase 2).
								Your worktree exists only to host the deterministic
								policy/diff-scope/verify gates that follow — re-run
								against the current state of \(_input.target), since it
								may have moved since the cached verdict was recorded.
								Do nothing else: run `rk done` immediately.
								"""
						}
					},
					{type: "wait", timeout: _input.reviewTimeout},
					// The gate-holder session must itself have finished cleanly.
					{type: "evaluate", expect: {is_error: false}},
				],
				_gates,
				(_routeVerdict & {Var: "cachedVerdict"}).steps,
			])
		},
	]

	// REDUCED TIER (see file header — TKT-01M0382WS1NNR0W5RA59PPDDYP /
	// TKT-01M036PSEHTMD3S5D2JFAG7XVY). Spawns a near-zero gate-holder instead
	// of a reviewer, runs the SAME gates, then lands unconditionally: there is
	// no verdict to route on because nothing reviewed the diff.
	_reducedArm: list.Concat([
		[
			{
				type:   "spawn"
				role:   "reviewer"
				agent:  "gateholder"
				branch: _input.branch
				task: {
					title: "steward-gatehold-" + _input.taskId
					description: """
						You are a gate-holder, not a reviewer. This diff (branch
						{{ctx.activeBranch}}, head \(_input.headSha)) was classified
						"\(_input.diffClass)" for \(_input.taskId), so the steward
						skipped the LLM review turn for it
						(TKT-01M036PSEHTMD3S5D2JFAG7XVY). Your worktree exists only to
						host the deterministic policy/diff-scope/verify gates that
						follow. Do nothing else: run `rk done` immediately.
						"""
				}
			},
			{type: "wait", timeout: _input.reviewTimeout},
			// The gate-holder session must itself have finished cleanly.
			{type: "evaluate", expect: {is_error: false}},
		],
		_gates,
		[
			// No verdict to read: the gates are the only gate this tier has.
			// AUTO-MERGE unconditionally once they pass, same delivery shape
			// (and the same fail-closed `delivered` guard) as the review tier's
			// APPROVE arm.
			{type: "dismiss", noMerge: true},
			{type: "land", branch: "{{ctx.activeBranch}}", target: _input.target},
			{type: "evaluate", expect: {delivered: true}},
		],
	])

	_needsReview: !(_input.diffClass == "doc-only" || _input.diffClass == "trivial")
	// PHASE 2 (rework): a cache probe needs BOTH headSha and branch to key on
	// — either missing (a legacy completion missing headSha, or a branchless
	// attach rat with no branch to bind) cannot safely probe the cache and
	// always takes the full review instead.
	_canProbeCache: _input.headSha != "" && _input.branch != ""

	steps: list.Concat([
		if !_needsReview {_reducedArm},
		if _needsReview && _canProbeCache {_cachedReviewArm},
		if _needsReview && !_canProbeCache {_reviewArm},
	])
}
