package product_to_code

#EvidenceKind: "impact" | "browser_acceptance" | "test_run" | "code_review" | "research_note" | "workflow_result" | "manual_observation"

#ProducerIdentity: {
	// Generic producer metadata. kind is intentionally open string, not tool-specific.
	kind:        string & !=""
	name:        string & !=""
	version?:    string | null
	invocation?: string | null
}

#BrowserAcceptancePayload: {
	url:          string & !=""
	scenario:     string & !=""
	steps:        [string & !="", ...(string & !="")]
	observations: [string & !="", ...(string & !="")]
}

#GenericEvidence: {
	id:              string & !=""
	kind:            #EvidenceKind
	producer:        #ProducerIdentity
	summary:         string & !=""
	artifact_paths?: [...string & !=""]
	if kind == "browser_acceptance" {
		payload: #BrowserAcceptancePayload
	}
	if kind != "browser_acceptance" {
		payload?: _
	}
}
