package paxos

import (
	"sort"
)

type node struct {
	id string
}

// SelfIndex is the position of the local node in the alphabetized list of all
// nodes in the cluster.
type cluster struct {
	self      string
	nodes     []string
	selfIndex int
}

func newCluster(self string, nodes map[string]string) *cluster {
	validNodes := make([]string, 0, len(nodes))
	ids := make(map[string]string)
	for id, addr := range nodes {
		if id != "" {
			validNodes = validNodes[0 : len(validNodes)+1]
			validNodes[len(validNodes)-1] = id
			ids[addr] = id
		}
	}

	sort.SortStrings(validNodes)
	selfIndex := -1
	for i, id := range validNodes {
		if id == self {
			selfIndex = i
		}
	}
	return &cluster{
		self:      self,
		nodes:     validNodes,
		selfIndex: selfIndex,
	}
}

func (cx *cluster) Len() int {
	return len(cx.nodes)
}

func (cx *cluster) Quorum() int {
	return cx.Len()/2 + 1
}

func (cx *cluster) SelfIndex() int {
	return cx.selfIndex
}

func (cx *cluster) indexOf(id string) int {
	for i, s := range cx.nodes {
		if s == id {
			return i
		}
	}
	return -1
}
