package product_to_code

#ArchitectureResearchArtifact: {
	id:                string & !=""
	initiative_id:     string & !=""
	researched_files:  [string & !="", ...(string & !="")]
	domain_terms?:     [...string]
	architecture_decisions: [...string & !=""]
	constraints?:      [...string]
	risks?:            [...string]
	open_questions:   [...string]
	open_questions_exhausted?: bool
	recommended_ticket_graph_path?: string | null
	evidence_ids?:     [...string & !=""]
}
