// action-approval-smoke-target: the harmless payload the weekly
// action-approval-boundary smoke check (scripts/rk-action-approval-smoke.py,
// TKT-01M08H9QQPJGFS9ET25Q26YSDM / program doc A3) proposes, approves, and
// executes through the factory approval boundary
// (crates/rk-daemon/src/action_approval.rs). It exists ONLY to be a safe
// `workflow.run` action: a single rat that does nothing but report done, then
// a no-merge dismiss. Never run this directly for real work — it is a
// no-op by design.
//
// WHY a real workflow.run instead of some lighter-weight action: the factory
// RPC surface (factory.propose_action / approve_action / execute_action) has
// exactly three FactoryAction kinds (workflow.run, ticket_graph.apply,
// product_to_code.dispatch); workflow.run is the only one with a harmless,
// side-effect-free shape. Executing it for real (not a dry run) is the point
// — the smoke check exists to prove the daemon still actually spawns and
// completes an approved run, not just that the digest math checks out.
//
// This file must resolve for the repo the smoke check targets (normally
// "rat-kingdom" itself), so it is committed both here (documentation) and at
// `.rk/workflows/action-approval-smoke-target.cue` (the live copy the daemon
// actually resolves — same pairing as implement-featureset.cue).
workflow: {
	name:        "action-approval-smoke-target"
	description: "no-op payload for the weekly action-approval-boundary smoke check"

	agents: {default: {harness: "codex"}}

	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "action-approval-smoke"
				description: """
					This is an automated no-op smoke check exercising the factory
					action-approval boundary (propose -> approve -> execute). There is
					nothing to implement, investigate, or change. Immediately run:
					  rk done "action-approval smoke ok"
					"""
			}
		},
		{type: "wait", timeout: "10m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss", noMerge: true},
	]
}
