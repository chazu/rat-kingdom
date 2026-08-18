// nightly-self-improve: the fleet's overnight self-improvement chain, expressed
// as ONE workflow so a single schedule fires it under a single single-flight
// lock. It runs three phases back to back, in the order the backlog wants them:
//
//   1. GROOM   — decompose / dedupe / tag the ticket backlog (mutates the ticket
//                store; nothing to merge). Runs first so phase 2 drains a
//                freshly-groomed queue.
//   2. DRAIN   — fan out one rat per ready ticket, run them in parallel, join,
//                then merge every clean branch in one sweep — the throughput dial.
//   3. REFINE  — mine the night's obstacle/need tuples and failed runs and
//                PROPOSE prompt + convention edits (proposals only). Runs last so
//                it sees the pain the drain just generated.
//
// This is the same three self-improvement loops shipped à la carte as
// backlog-groom / backlog-drain / prompt-refine, welded into one instance. Why
// weld rather than run three separate schedules? So the WHOLE night is one unit
// of work behind one single-flight lock: a slow drain can never let the next
// night's groom start on top of it (see docs/scheduler.md — the scheduler keys
// its lock on the schedule name, and one schedule = one instance here). If you'd
// rather each phase be independently retryable, schedule the three phases
// separately instead (examples/schedules.cue shows both).
//
//   rk workflow run nightly-self-improve
//   rk workflow run nightly-self-improve --param budgetUsd=40
//
// Scheduled nightly via examples/schedules.cue. Overnight cost is bounded by the
// per-instance `budget` cap below (#WorkflowBudget) — a wallet kill-switch
// scoped to THIS night. Once this run's summed agent cost reaches it, the same
// pre-dispatch guard every spawn passes refuses further dispatch. This layers
// below the global fleet/repo caps (rk_ledger::FleetBudget), which are
// fleet-wide, not scoped to one run — so without a per-instance cap a single
// runaway night could consume the whole fleet's cap.
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "nightly-self-improve"
	description: "overnight chain: groom the backlog, drain it in parallel, then propose prompt/convention refinements"

	params: {
		// Cap on how many ready tickets the drain phase works in one pass.
		limit: {type: "int", required: false, default: 5}
		// Drain join timeout — every drained rat must finish within this window.
		timeout: {type: "string", required: false, default: "45m"}
		// Repo base branch. The refine phase pins its spawn to this so its
		// proposals land on the base, not chained onto the groom branch — see the
		// note on that spawn below.
		base: {type: "string", required: false, default: "main"}
		// Per-instance wallet cap in whole USD for the WHOLE night (groom + drain
		// + refine). Once this run's summed agent cost reaches it, further
		// dispatch is refused — below the global fleet/repo caps. This is what
		// bounds THIS night specifically; the fleet/repo caps are fleet-wide, so
		// without it a single runaway night could eat the whole fleet cap.
		// (max_usd is a number; params are int/string/bool only, so this is an
		// int-dollar param.)
		budgetUsd: {type: "int", required: false, default: 30}
	}

	agents: {default: {harness: "codex"}}

	// Wallet kill-switch scoped to this one overnight instance.
	budget: {max_usd: _input.budgetUsd}

	steps: [
		// ── Phase 1: GROOM ──────────────────────────────────────────────────
		// Grooming mutates the ticket store, not the worktree, so it dismisses
		// with noMerge. There is deliberately no evaluate gate here: a groom
		// hiccup is low-stakes and must NOT abort the night — the drain should
		// still run against whatever ready tickets exist.
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "groom-backlog"
				description: """
					Read the open backlog:  rk ticket list --status open
					For each oversized ticket, decompose it:
					  rk ticket new "<sub>" --parent <TKT-id>
					Merge duplicates (note the survivor in each), and flag stale items.
					Do NOT start any ticket. Record what you changed:
					  rk out artifact $RK_REPO backlog-groom --payload '{"decomposed": N, "deduped": M}'
					Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		{type: "dismiss", noMerge: true},

		// ── Phase 2: DRAIN ──────────────────────────────────────────────────
		// Fan out one rat per ready ticket; the title defaults to {{item.id}} so
		// the supervisor drives each ticket's status lifecycle. The evaluate
		// gates the batch merge: if any rat errored, the instance fails and
		// dismiss_all never runs, so a broken batch parks for manual review
		// rather than auto-merging (this also skips phase 3 for that night — the
		// failed instance surfaces in `rk inbox`).
		{
			type: "for_each"
			query: {status: "ready", limit: _input.limit}
			role: "rat"
			task: {
				title: "{{item.id}}"
				description: """
					Work ticket {{item.id}}: {{item.title}}

					{{item.body}}

					This is your only task. Implement it end-to-end on your branch,
					commit your work, run the repo's tests, then run `rk done`.
					"""
			}
		},
		{type: "wait_all", timeout: _input.timeout},
		{type: "evaluate", expect: {all_ok: true}},
		{type: "dismiss_all"},

		// ── Phase 3: REFINE ─────────────────────────────────────────────────
		// Mine the night's pain and propose (never apply) prompt/convention
		// edits. Proposals are low-risk (docs/proposals + tickets), so the merge
		// is gated only on the rat finishing cleanly.
		//
		// `branch: _input.base` pins this spawn to the repo base rather than
		// letting it chain onto ctx.active_branch (which still points at the
		// groom branch — a single `dismiss` clears the active AGENT but not the
		// active BRANCH, by design, so chained workflows keep chaining). Without
		// this the refine dismiss would merge its proposals back into the stale
		// groom branch instead of the base, and they would never land.
		{
			type:   "spawn"
			role:   "rat"
			branch: _input.base
			task: {
				title: "refine-prompts"
				description: """
					Scan recurring pain:
					  rk scan obstacle ; rk scan need ; rk scan fact system
					Cross-reference with recent workflow_failed events and tonight's drain.
					Classify each failure before proposing a prompt change. A prompt
					candidate requires evidence that the relevant tool/model/workflow
					gate was available and that the current role instructions caused
					the observed behavior. Missing executables, inaccessible models,
					workflow-policy refusals, daemon authorization errors, merge
					collisions, and repository-check failures are infrastructure,
					workflow, or gate findings unless evidence proves otherwise.
					For those boundaries, do not write a speculative prompt proposal:
					file one ticket with the exact run id, step, error, and owning
					boundary, then continue mining other evidence.
					Where a recurring failure traces to a weak role prompt or a missing
					convention, propose a concrete edit (write a diff/patch under
					docs/proposals/prompts/) and, for durable rules, propose a convention:
					  rk out artifact $RK_REPO convention-proposal --payload '{"rule": "...", "why": "..."}'
					File a ticket for each proposed change. Do NOT edit live prompts.
					Commit proposals, report done.
					"""
			}
		},
		// The refine phase mines the full event feed and writes proposals; its
		// observed cost exceeds the old 25m budget, so match the fan-out join.
		{type: "wait", timeout: "45m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss"},
	]
}
