package paxos

const (
	prefixKey = "/j"
	clusterKey = prefixKey+"/junta"
	membersKey = clusterKey+"/members"
	membersDir = membersKey+"/"
	slotKey    = clusterKey+"/slot"
	slotDir    = slotKey+"/"
)
