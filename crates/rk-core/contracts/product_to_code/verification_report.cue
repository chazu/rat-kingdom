package product_to_code

#AcceptanceCriterionVerification: {
	acceptance_criterion_id: string & !=""
	status:                  string & !=""
	evidence_ids:            [string & !="", ...(string & !="")]
	notes?:                  string | null
}

#VerificationReport: {
	id:            string & !=""
	initiative_id: string & !=""
	entries:       [#AcceptanceCriterionVerification, ...#AcceptanceCriterionVerification]
}
