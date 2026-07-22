// gated-merge: a rat implements a change, then the branch parks behind a
// HUMAN APPROVAL GATE. On approval the branch auto-merges; on rejection (or if
// no human responds before the timeout) the run fails and the branch is left
// unmerged for review. The safety valve that makes broad unattended autonomy
// palatable — a human veto exactly at the merge boundary.
//
//   rk workflow run gated-merge --param taskId=risky-change \
//     --param description="Rework the payment retry logic"
//
// While parked, inspect the branch, then decide:
//   rk workflow list                 # find the instance id (wf-...)
//   rk approve <instance>            # -> auto-merges
//   rk reject  <instance>            # -> run fails, branch held unmerged
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "gated-merge"
	description: "rat implements; a human approval gate decides whether the branch merges"
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
		// instance, or approvalTimeout elapses (then {approved: false}). The
		// rat is NOT dismissed here, so its branch/worktree survive for review.
		{type: "gate", gateType: "approval", timeout: _input.approvalTimeout},
		// Proceed only on an explicit approval; rejection/timeout fails the run
		// and leaves the branch unmerged.
		{type: "evaluate", expect: {approved: true}},
		{type: "dismiss"}, // merge on approval
	]
}
