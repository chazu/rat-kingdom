package product_to_code

#AcceptanceCriterion: {
	id:   string & !=""
	text: string & !=""
}

#InitiativeContract: {
	id:                             string & !=""
	title:                          string & !=""
	scope:                          string & !=""
	browser_acceptance_applicable: bool
	acceptance_criteria: [#AcceptanceCriterion, ...#AcceptanceCriterion]
}
