package paxos

import (
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
	outs      PutterTo
}

func newCluster(self string, addrsById map[string]string, active []string, outs PutterTo) *cluster {
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
		outs:      outs,
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

func (cx *cluster) Put(m Msg) {
	for addr := range cx.idsByAddr {
		cx.outs.PutTo(m, addr)
	}
}
