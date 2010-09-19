package paxos

const (
	clusterKey = "/junta"
	membersKey = clusterKey + "/members"
	membersDir = membersKey + "/"
	slotKey    = clusterKey + "/slot"
	slotDir    = slotKey + "/"
)
