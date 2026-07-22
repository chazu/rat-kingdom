// research: one rat investigates a question, writes its findings to a Markdown
// document, commits it to its worktree, and reports done. The document is
// merged into the base branch on success.
//
//   rk workflow run research \
//     --param question="How does the tuplespace handle concurrent writers?" \
//     --param outfile=docs/research/tuplespace-concurrency.md
//
// Copy to ~/.rat-kingdom/workflows/ (global) or <repo>/.rk/workflows/.
workflow: {
	name:        "research"
	description: "one rat researches a question, writes+commits a document, reports done"

	params: {
		question: {type: "string", required: true}
		// Where the rat writes its findings, relative to the repo root.
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
					Research and answer the following question, then write your
					findings as a well-structured Markdown document.

					Question:
					\(_input.question)

					Deliverable:
					- Write the document to \(_input.outfile) (create parent dirs
					  as needed). Include the question, a direct answer, the
					  evidence/reasoning behind it, and any open questions.
					- Do not modify unrelated files — this is a research task,
					  not a code change.
					- Commit the document to your worktree:
					    git add \(_input.outfile)
					    git commit -m "research: \(_input.question)"
					- Then report that you are done.
					"""
			}
		},
		{type: "wait", timeout: "30m"},
		// The harness result must not be an error.
		{type: "evaluate", expect: {is_error: false}},
		// Merge the research document into the base branch and clean up.
		{type: "dismiss"},
	]
}
