package product_to_code

#NonBlankString: string & =~"\\S"

#ArchitectureResearchArtifact: {
	id:               #NonBlankString
	initiative_id:    #NonBlankString
	researched_files: [#NonBlankString, ...#NonBlankString]
	domain_terms?:    [...#NonBlankString]
	architecture_decisions?: [...#NonBlankString]
	constraints?:     [...#NonBlankString]
	risks?:           [...#NonBlankString]
	recommended_ticket_graph_path?: #NonBlankString | null
	evidence_ids?:    [...#NonBlankString]
} & ({
	open_questions: [#NonBlankString, ...#NonBlankString]
	open_questions_exhausted?: bool
} | {
	open_questions?: []
	open_questions_exhausted: true
})
