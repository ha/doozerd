package paxos

import (
    "sort"
)

type Node struct {
    id string
}

// TODO this is a bad name. I predict that we should soon get rid of the
// "Cluster" *interface* and rename this type to "Cluster".
type RealCluster struct {
    self string
    nodes []string
    fw Putter
    selfIndex int
}

func NewCluster(self string, nodes []string, fw Putter) Cluster {
    sort.SortStrings(nodes)
    selfIndex := -1
    for i, id := range nodes {
        if id == self {
            selfIndex = i
        }
    }
    return &RealCluster{
        self: self,
        nodes: nodes,
        fw: fw,
        selfIndex: selfIndex,
    }
}

func (cx *RealCluster) Len() int {
    return len(cx.nodes)
}

func (cx *RealCluster) Quorum() int {
    return cx.Len()/2 + 1
}

func (cx *RealCluster) SelfIndex() int {
    return cx.selfIndex
}

func (cx *RealCluster) Put(m Message) {
    cx.fw.Put(m)
}
