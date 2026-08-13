// research: one rat investigates a question, writes a structured architecture
// research artifact plus Markdown rendering, validates the artifact locally,
// commits both deliverables, and reports done. The documents are merged into
// the base branch on success.
//
//   rk workflow run research \
//     --param question="How does the tuplespace handle concurrent writers?" \
//     --param initiative=docs/product-to-code/initiative.json \
//     --param artifact=docs/research/tuplespace-concurrency.json \
//     --param outfile=docs/research/tuplespace-concurrency.md
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "research"
	description: "one rat researches a question, writes+validates ArchitectureResearchArtifact JSON, renders Markdown, reports done"

	params: {
		question: {type: "string", required: true}
		// InitiativeContract JSON path, relative to the repo root.
		initiative: {type: "string", required: true}
		// ArchitectureResearchArtifact JSON output path, relative to the repo root.
		artifact: {type: "string", required: false, default: "docs/research/architecture-research.json"}
		// Rendered Markdown output path, relative to the repo root.
		outfile: {type: "string", required: false, default: "docs/research/findings.md"}
	}

	agents: {
		default: {harness: "claude"}
	}

	steps: [
		{
			type: "spawn"
			role: "rat"
			task: {
				title: "research"
				description: """
					Research and answer the following question, then write a structured
					ArchitectureResearchArtifact JSON file and deterministic Markdown rendering.

					Question:
					\(_input.question)

					Required structured artifact:
					- Write ArchitectureResearchArtifact JSON to \(_input.artifact).
					- The artifact must reference initiative_id from \(_input.initiative).
					- Include at least one concrete repo-relative researched_files path.
					- Include architecture substance in architecture_decisions, constraints, or risks.
					- Include open_questions or set open_questions_exhausted: true.
					- Include recommended_ticket_graph_path when a ticket graph is ready or null otherwise.

					Validation command, required before reporting completion:
					  rk --json product-to-code research validate --artifact \(_input.artifact) --initiative \(_input.initiative)

					Rendered Markdown:
					  rk product-to-code research render --artifact \(_input.artifact) --format markdown > \(_input.outfile)

					Do not modify unrelated files. This is a local research task, not a daemon mutation.

					Commit only the artifact and rendered document:
					  git add \(_input.artifact) \(_input.outfile)
					  git commit -m "research: \(_input.question)"

					Then report that you are done.
					"""
			}
		},
		{type: "wait", timeout: "30m"},
		// The harness result must not be an error.
		{type: "evaluate", expect: {is_error: false}},
		// Merge the research artifact and document into the base branch and clean up.
		{type: "dismiss"},
	]
}
