// gated-merge: a rat implements a change, then the branch parks behind a
// HUMAN APPROVAL GATE. On approval the branch auto-merges; on rejection the
// branch is torn down cleanly but left UNMERGED for review — and the run
// COMPLETES rather than failing. The safety valve that makes broad unattended
// autonomy palatable: a human veto exactly at the merge boundary, where a "no"
// is a normal outcome, not an error.
//
//   rk workflow run gated-merge --param taskId=risky-change \
//     --param description="Rework the payment retry logic"
//
// While parked, inspect the branch, then decide:
//   rk workflow list                 # find the instance id (wf-...)
//   rk approve <instance>            # -> auto-merges, run completes
//   rk reject  <instance>            # -> branch held unmerged, run completes
//
// The gate writes the decision {approved, by, reason} into ctx.previousResult
// AND records it as a `workflow_approval` event; a `read` lifts `approved` into
// ctx.var.approved and a `when` routes on it. No decision before
// approvalTimeout fails closed (treated as not-approved -> held unmerged).
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "gated-merge"
	description: "rat implements; a human approval gate routes merge vs. clean hold"
	params: {
		taskId: {type: "string", required: true}
		description: {type: "string", required: false, default: ""}
		// How long the rat gets to implement before the wait times out.
		implTimeout: {type: "string", required: false, default: "30m"}
		// How long to hold the branch waiting for a human decision. No
		// response by then fails closed (treated as not-approved).
		approvalTimeout: {type: "string", required: false, default: "24h"}
	}
	agents: {
		default: {harness: "claude"}
	}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {title: _input.taskId, description: _input.description}
		},
		{type: "wait", timeout: _input.implTimeout},
		{type: "evaluate", expect: {is_error: false}}, // implementation must succeed
		// Park behind the human. Blocks until `rk approve`/`rk reject` for this
		// instance, or approvalTimeout elapses (then a fail-closed
		// {approved: false}). The rat is NOT dismissed here, so its
		// branch/worktree survive for the merge decision.
		{type: "gate", gateType: "approval", timeout: _input.approvalTimeout},
		// Lift the human's verdict into ctx.var.approved so `when` can route on
		// it. The gate leaves a `workflow_approval` event behind (both for a
		// real decision and for the fail-closed timeout), so this read resolves
		// immediately rather than blocking a second time.
		//
		// `fromInstance` binds it to THIS run's decision (TKT-172). The gate
		// above already waits per-instance, but (event, <repo>,
		// workflow_approval) is shared by every gated instance on the repo, so
		// an unbound read takes the newest decision in the scope whoever it was
		// meant for: approve this one while a peer is rejected and the `when`
		// below holds this branch on the peer's veto — or merges it on the
		// peer's approval.
		{
			type:         "read"
			category:     "event"
			identity:     "workflow_approval"
			fromInstance: true
			field:        "approved"
			into:         "approved"
			timeout:      "5m"
		},
		// Route the decision. Either way the run ENDS CLEANLY — the difference
		// is only whether the branch merges.
		{
			type: "when"
			var:  "approved"
			cases: {
				// Approved: dismiss merges the branch into the target and
				// deletes it. The run completes.
				"true": [
					{type: "dismiss"},
				]
				// Rejected (or fail-closed timeout): dismiss --no-merge tears
				// down the worktree but PRESERVES the branch, unmerged, for a
				// human to inspect. The run still completes — a veto is a
				// normal outcome, not a failure.
				"false": [
					{type: "dismiss", noMerge: true},
				]
			}
			// An unrecognized decision value is a bug, not a veto: hold the
			// branch unmerged and abort loudly rather than silently merging.
			default: [
				{type: "dismiss", noMerge: true},
				{type: "stop", reason: "unrecognized approval decision for " + _input.taskId},
			]
		},
	]
}
