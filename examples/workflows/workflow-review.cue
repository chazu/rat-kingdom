// workflow-review: one rat audits the workflow library and proposes refined
// CUE definitions plus follow-up tickets, without editing the shipped files.
//
//   rk workflow run workflow-review \
//     --param focus="all shipped workflows"
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "workflow-review"
	description: "a rat audits the workflow library and proposes refined CUE + tickets"
	params: {
		focus: {type: "string", required: false, default: "all shipped workflows"}
	}
	agents: {default: {harness: "claude"}}
	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "review-workflows"
				description: """
					Read crates/rk-workflow/src/schema.cue and every file under
					examples/workflows/. Focus: \(_input.focus).

					For each workflow: assess correctness against the schema, missing
					guardrails (evaluate/dismiss), and leverage. Where you can improve a
					definition, write the revised .cue to docs/proposals/ (do NOT edit the
					shipped files). File a ticket per substantive change:
					  rk ticket new "<title>" --body "<rationale>"
					Record a summary artifact:
					  rk out artifact $RK_REPO workflow-review --payload '{"reviewed": N, "tickets": M}'
					Commit your proposals, then report done.
					"""
			}
		},
		{type: "wait", timeout: "30m"},
		{type: "evaluate", expect: {is_error: false}},
		{type: "dismiss"}, // merge the proposals doc
	]
}
