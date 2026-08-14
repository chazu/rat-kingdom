// product-to-code: compose the full Phase 3 product-to-code pipeline as ONE
// workflow. Rat Kingdom stays the authority at every step: the workflow only
// validates offline artifacts, proposes typed daemon actions, and dispatches
// implementation AFTER the operator approved the canonical proposals.
//
// Composition order (each step gates the next):
//   1. validate the initiative contract (implied by every validation below);
//   2. validate the architecture research artifact:  rk product-to-code research validate
//   3. validate the ticket graph:                    rk product-to-code graph validate
//   4. propose the approved graph apply:             rk product-to-code graph propose-apply
//      (an operator approves and executes the canonical ticket_graph.apply
//       proposal out-of-band; that mints TKT-... ids for graph nodes)
//   5. propose implement-featureset dispatch for unblocked nodes:
//                                                    rk product-to-code workflow propose
//      (the daemon's product_to_code.dispatch executor runs
//       `rk workflow run implement-featureset --param taskId=TKT-... \
//          --param taskDescription=...` semantics for each unblocked minted
//       ticket after operator approval; blocked nodes without impact evidence
//       are listed separately and never dispatched)
//   6. block delivery until independent verification passes by composing the
//      shipped `independent-verifier` workflow, which maps every acceptance
//      criterion to evidence or an explicit gap and has no implementation
//      authority.
//
//   rk workflow run product-to-code \
//     --param initiative=docs/product-to-code/initiative.json \
//     --param research=docs/research/architecture-research.json \
//     --param graph=docs/product-to-code/ticket-graph.json \
//     --param evidenceDir=docs/product-to-code/evidence
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "product-to-code"
	description: "validate research and graph, propose approved graph apply and dispatch, gate delivery on independent verification"

	params: {
		initiative: {type: "string", required: false, default: "docs/product-to-code/initiative.json"}
		research: {type: "string", required: false, default: "docs/research/architecture-research.json"}
		graph: {type: "string", required: false, default: "docs/product-to-code/ticket-graph.json"}
		evidenceDir: {type: "string", required: false, default: "docs/product-to-code/evidence"}
		repo: {type: "string", required: false, default: "."}
		verifyTaskId: {type: "string", required: false, default: "product-to-code-delivery"}
		verifyDescription: {type: "string", required: false, default: "Independently verify the delivered product-to-code initiative"}
		approvalTimeout: {type: "string", required: false, default: "24h"}
	}

	agents: {default: {harness: "claude"}}

	steps: [
		// ── 1-2. Research gate: the structured architecture research artifact
		// must validate against the initiative before any ticket planning.
		{
			type: "run"
			command: "rk --json product-to-code research validate --artifact \(_input.research) --initiative \(_input.initiative)"
			expectExit: 0
			timeout: "2m"
		},
		// ── 3. Graph gate: missing dependencies and cycles fail closed here,
		// before any proposal exists.
		{
			type: "run"
			command: "rk --json product-to-code graph validate --graph \(_input.graph) --initiative \(_input.initiative)"
			expectExit: 0
			timeout: "2m"
		},
		// ── 4. Propose the canonical ticket_graph.apply. The daemon owns the
		// proposal; nothing is applied until an operator approves the exact
		// digest and executes it.
		{
			type: "run"
			command: "rk --json product-to-code graph propose-apply --graph \(_input.graph) --initiative \(_input.initiative) --repo \(_input.repo)"
			expectExit: 0
			timeout: "2m"
		},
		// The operator approves and executes the graph apply out-of-band
		// (rk factory approve <proposal> <digest>; rk factory execute ...).
		// This human gate holds the workflow until that happened.
		{type: "gate", gateType: "approval", timeout: _input.approvalTimeout},
		// ── 5. Propose implement-featureset dispatch for unblocked nodes. The
		// command builds the canonical product_to_code.dispatch proposal from
		// the approved graph apply's graph-node-id -> TKT-id mapping. Nodes
		// without current impact evidence are listed as blocked and are never
		// dispatched. Approval and execution again belong to the daemon:
		// its executor runs `rk workflow run implement-featureset
		// --param taskId=TKT-... --param taskDescription=...` per unblocked
		// minted ticket.
		{
			type: "run"
			command: "rk --json product-to-code workflow propose --initiative \(_input.initiative) --research \(_input.research) --graph \(_input.graph) --evidence-dir \(_input.evidenceDir) --repo \(_input.repo)"
			expectExit: 0
			timeout: "2m"
		},
		// The operator approves the dispatch proposal; the daemon then runs the
		// implement-featureset workflows for unblocked tickets.
		{type: "gate", gateType: "approval", timeout: _input.approvalTimeout},
		// ── 6. Delivery is blocked until independent verification passes.
		// Compose the shipped independent-verifier workflow: it maps every
		// acceptance criterion to evidence or an explicit gap, has no
		// implementation authority, and its evaluate gate fails this workflow
		// when verification fails.
		{
			type:     "sub_workflow"
			workflow: "independent-verifier"
			params: {
				taskId:      "\(_input.verifyTaskId)"
				description: "\(_input.verifyDescription)"
			}
		},
		{type: "evaluate", expect: {is_error: false}},
	]
}
