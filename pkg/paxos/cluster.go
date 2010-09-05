package paxos

import (
	"container/vector"
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
	idsByAddr map[string]string
	addrsById map[string]string
}

func newCluster(addrsById map[string]string) *cluster {
	self := ""
	validNodes := make([]string, 0, len(addrsById))
	idsByAddr := make(map[string]string)
	addrsByIdCopy := make(map[string]string)
	for id, addr := range addrsById {
		if id != "" {
			validNodes = validNodes[0 : len(validNodes)+1]
			validNodes[len(validNodes)-1] = id
			idsByAddr[addr] = id
			addrsByIdCopy[id] = addr
		}
	}

	sort.SortStrings(validNodes)
	return &cluster{
		self:      self,
		nodes:     validNodes,
		idsByAddr: idsByAddr,
		addrsById: addrsByIdCopy,
	}
}

func (cx *cluster) Len() int {
	return len(cx.nodes)
}

func (cx *cluster) Quorum() int {
	return cx.Len()/2 + 1
}

func (cx *cluster) SelfIndex() int {
	return cx.indexById(cx.self)
}

func (cx *cluster) indexById(id string) int {
	for i, s := range cx.nodes {
		if s == id {
			return i
		}
	}
	return -1
}

func (cx *cluster) idByAddr(addr string) string {
	return cx.idsByAddr[addr]
}

func (cx *cluster) indexByAddr(addr string) int {
	return cx.indexById(cx.idByAddr(addr))
}

func (cx *cluster) addrs() (addrs []string) {
	for addr, _ := range cx.idsByAddr {
		(*vector.StringVector)(&addrs).Push(addr)
	}
	return
}
