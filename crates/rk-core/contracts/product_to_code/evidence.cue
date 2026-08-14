package product_to_code

#EvidenceKind: "impact" | "browser_acceptance" | "test_run" | "code_review" | "research_note" | "workflow_result" | "manual_observation"
#NonBlankString: string & =~"\\S"

#ProducerIdentity: {
	// Generic producer metadata. kind is intentionally open string, not tool-specific.
	kind:        #NonBlankString
	name:        #NonBlankString
	version?:    string | null
	invocation?: string | null
}

#BrowserAcceptancePayload: {
	url:          #NonBlankString
	scenario:     #NonBlankString
	steps:        [#NonBlankString, ...#NonBlankString]
	observations: [#NonBlankString, ...#NonBlankString]
}

#GenericEvidence: {
	id:       #NonBlankString
	kind:     #EvidenceKind
	producer: #ProducerIdentity
	summary:  #NonBlankString
	if kind == "browser_acceptance" {
		artifact_paths: [#NonBlankString, ...#NonBlankString]
		payload:        #BrowserAcceptancePayload
	}
	if kind != "browser_acceptance" {
		artifact_paths?: [...#NonBlankString]
		payload?:        _
	}
}
