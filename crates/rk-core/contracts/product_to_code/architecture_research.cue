package product_to_code

#ArchitectureResearchArtifact: {
	id:               string & !=""
	initiative_id:    string & !=""
	researched_files: [string & !="", ...(string & !="")]
	domain_terms?:    [...string]
	architecture_decisions: [string & !="", ...(string & !="")]
	constraints?:     [...string]
	risks?:           [...string]
	recommended_ticket_graph_path?: string | null
	evidence_ids?:    [...string & !=""]
} & ({
	open_questions: [string & !="", ...(string & !="")]
	open_questions_exhausted?: bool
} | {
	open_questions?: []
	open_questions_exhausted: true
})
