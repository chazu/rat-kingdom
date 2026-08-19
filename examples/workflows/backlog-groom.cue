// backlog-groom: one rat decomposes oversized tickets and dedupes/flags them
// for the operator, then a groomer closes exactly the stale/duplicate tickets
// it can back with recorded evidence. Neither step starts any ticket, and the
// groomer's harness is forced read-only — grooming mutates the ticket store,
// not the worktree.
//
//   rk workflow run backlog-groom
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "backlog-groom"
	description: "one rat decomposes/flags, then a groomer closes evidence-backed stale tickets"
	agents: {default: {harness: "claude"}}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "groom-backlog"
				description: """
					Read the open backlog:  rk ticket list --status open
					For each oversized ticket, decompose it:
					  rk ticket new "<sub>" --parent <TKT-id>
					Note duplicate clusters (with a recommended survivor) and any ticket
					that looks stale, but do NOT close or start any ticket yourself —
					ticket.update is not available to this role. Record what you found:
					  rk out artifact $RK_REPO backlog-groom --payload '{"decomposed": N, "duplicates": [...], "stale_candidates": [...]}'
					Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},   // no code change; ticket store mutated directly
		{
			type: "spawn"
			role: "groomer"
			task: {
				title: "close-evidence-backed-tickets"
				description: """
					Read the open and in_progress backlog, including anything the prior
					decompose/flag pass recorded (rk scan artifact $RK_REPO backlog-groom).
					For each candidate, gather evidence before acting:
					- rework: TKT-... tickets — verify with `rk ticket show <target>` that
					  the target is done AND its fix actually landed (git log --grep or
					  git merge-base --is-ancestor against the integration branch).
					- Reported test failures — re-run the named test ONCE with the RK_*
					  spawn env stripped and confirm it now passes.
					- Duplicate clusters — confirm the exact same symptom and pick the
					  survivor with the most complete root-cause analysis.
					Close ONLY what you can back with evidence:
					  rk ticket update <TKT-id> --status closed --reason "<stale-rework|stale-flake|duplicate>" --evidence "<what you verified>"
					Leave anything ambiguous open and hand it off instead:
					  rk out artifact $RK_REPO backlog-groom --payload '{"closed": N, "handed_off": M}'
					Report done.
					"""
			}
		},
		{type: "wait", timeout: "20m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},   // no code change; ticket store mutated directly
	]
}
