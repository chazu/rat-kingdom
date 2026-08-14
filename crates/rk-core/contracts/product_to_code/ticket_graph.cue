package product_to_code

#NonBlankString: string & =~"\\S"

#TicketGraphNode: {
	id:          #NonBlankString
	title:       #NonBlankString
	description: #NonBlankString
	acceptance_criterion_ids?: [...#NonBlankString]
}

#TicketGraphEdge: {
	from:         #NonBlankString
	to:           #NonBlankString
	relationship: #NonBlankString
}

#TicketGraph: {
	id:            #NonBlankString
	initiative_id: #NonBlankString
	nodes: [#TicketGraphNode, ...#TicketGraphNode]
	edges?: [...#TicketGraphEdge]
}
