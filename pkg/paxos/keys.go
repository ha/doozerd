package paxos

const (
	prefixKey = "/j"
	clusterKey = prefixKey+"/junta"
	membersKey = clusterKey+"/members"
	membersDir = membersKey+"/"
	slotKey    = prefixKey+"/slot"
	slotDir    = slotKey+"/"
)
