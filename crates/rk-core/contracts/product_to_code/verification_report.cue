package product_to_code

#NonBlankString: string & =~"\\S"

#AcceptanceCriterionVerification: {
	acceptance_criterion_id: #NonBlankString
	status:                  "satisfied" | "partially_satisfied" | "not_satisfied" | "not_applicable" | "pass" | "passed" | "partial" | "fail" | "failed"
	evidence_ids?: [...#NonBlankString]
	notes?: string | null
	gap?: #NonBlankString | null
}

#VerificationReport: {
	id:            #NonBlankString
	initiative_id: #NonBlankString
	verifier?:     #NonBlankString | null
	scope?:        #NonBlankString | null
	entries: [#AcceptanceCriterionVerification, ...#AcceptanceCriterionVerification]
	recommendation?: #NonBlankString | null
}
