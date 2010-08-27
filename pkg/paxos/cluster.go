package paxos

import (
    "sort"
)

type Node struct {
    id string
}

// SelfIndex is the position of the local node in the alphabetized list of all
// nodes in the cluster.
type cluster struct {
    self string
    nodes []string
    selfIndex int
}

func NewCluster(self string, nodes []string) *cluster {
    sort.SortStrings(nodes)
    selfIndex := -1
    for i, id := range nodes {
        if id == self {
            selfIndex = i
        }
    }
    return &cluster{
        self: self,
        nodes: nodes,
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
