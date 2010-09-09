package paxos

import (
	"container/vector"
	"sort"
)

type node struct {
	id string
}

type cluster struct {
	self      string
	active    []string
	idsByAddr map[string]string
	addrsById map[string]string
}

func stringKeys(m map[string]string) []string {
	keys := make([]string, len(m))
	i := 0
	for k, _ := range m {
		keys[i] = k
		i++
	}
	return keys
}

func newCluster(self string, addrsById map[string]string) *cluster {
	active := stringKeys(addrsById)

	idsByAddr := make(map[string]string)
	for id, addr := range addrsById {
		idsByAddr[addr] = id
	}

	sort.SortStrings(active)
	return &cluster{
		self:      self,
		active:    active,
		idsByAddr: idsByAddr,
		addrsById: addrsById,
	}
}

func (cx *cluster) Len() int {
	return len(cx.active)
}

func (cx *cluster) Quorum() int {
	return cx.Len()/2 + 1
}

func (cx *cluster) SelfIndex() int {
	return cx.indexById(cx.self)
}

func (cx *cluster) indexById(id string) int {
	for i, s := range cx.active {
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
