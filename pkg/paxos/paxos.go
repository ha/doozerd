package paxos

// TODO maybe we can make a better name for this. Not sure.
//
// SelfIndex is the position of the local node in the alphabetized list of all
// nodes in the cluster.
type Cluster interface {
	Putter
	Len() int
	Quorum() int
	SelfIndex() int
}
