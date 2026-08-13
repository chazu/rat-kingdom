package product_to_code

#NonBlankString: string & =~"\\S"
#SafeRepoRelativePath: #NonBlankString & !~"(^/|(^|/)\\.\\.(/|$))"

#ArchitectureResearchArtifact: {
	id:               #NonBlankString
	initiative_id:    #NonBlankString
	researched_files: [#SafeRepoRelativePath, ...#SafeRepoRelativePath]
	domain_terms?:    [...#NonBlankString]
	architecture_decisions: [#NonBlankString, ...#NonBlankString]
	constraints:     [#NonBlankString, ...#NonBlankString]
	risks:           [#NonBlankString, ...#NonBlankString]
	recommended_ticket_graph_path?: #SafeRepoRelativePath | null
	evidence_ids?:    [...#NonBlankString]
} & ({
	open_questions: [#NonBlankString, ...#NonBlankString]
	open_questions_exhausted?: bool
} | {
	open_questions?: []
	open_questions_exhausted: true
})
