package product_to_code

#NonBlankString: string & =~"\\S"

#AcceptanceCriterion: {
	id:   #NonBlankString
	text: #NonBlankString
}

#InitiativeContract: {
	id:                             #NonBlankString
	title:                          #NonBlankString
	scope:                          #NonBlankString
	browser_acceptance_applicable: bool
	acceptance_criteria: [#AcceptanceCriterion, ...#AcceptanceCriterion]
}
