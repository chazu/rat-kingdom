package product_to_code

#NonBlankString: string & =~"\\S"

#AcceptanceCriterionVerification: {
	acceptance_criterion_id: #NonBlankString
	status:                  #NonBlankString
	evidence_ids: [#NonBlankString, ...#NonBlankString]
	notes?: string | null
}

#VerificationReport: {
	id:            #NonBlankString
	initiative_id: #NonBlankString
	entries: [#AcceptanceCriterionVerification, ...#AcceptanceCriterionVerification]
}
