package product_to_code

#TicketGraphNode: {
	id:          string & !=""
	title:       string & !=""
	description: string & !=""
	acceptance_criterion_ids?: [...string & !=""]
}

#TicketGraphEdge: {
	from:         string & !=""
	to:           string & !=""
	relationship: string & !=""
}

#TicketGraph: {
	id:            string & !=""
	initiative_id: string & !=""
	nodes:         [#TicketGraphNode, ...#TicketGraphNode]
	edges?:        [...#TicketGraphEdge]
}
