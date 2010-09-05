package paxos

import (
	"junta/store"
	"container/heap"
	"container/vector"
	"math"
	"strings"
)

type Registrar struct {
	window   int
	st       *store.Store
	evs      chan store.Event
	lookupCh chan lookup
	lookups  *lookupQueue
}

type lookup struct {
	cver uint64
	ch   chan *cluster
}

func (l lookup) Less(y interface{}) bool {
	return l.cver < y.(lookup).cver
}

type lookupQueue struct {
	vector.Vector
}

func (q *lookupQueue) peek() lookup {
	if q.Len() == 0 {
		return lookup{cver: math.MaxUint64} // ~infinity
	}
	return q.At(0).(lookup)
}

// This thing keeps track of who is supposed to be in the cluster for every
// seqn. It also remembers the network address of every member.
func NewRegistrar(st *store.Store, start uint64, window int) *Registrar {
	rg := &Registrar{
		window:   window,
		st:       st,
		evs:      make(chan store.Event),
		lookupCh: make(chan lookup),
		lookups:  new(lookupQueue),
	}
	heap.Init(rg.lookups)
	st.Watch(membersKey, store.Add|store.Rem, rg.evs)
	st.WatchApply(rg.evs)
	go rg.process(start, members(st))
	return rg
}

func findString(v []string, s string) (i int) {
	for i = range v {
		if v[i] == s {
			return
		}
	}
	return -1
}

func (rg *Registrar) process(known uint64, members map[string]string) {
	clusters := make(map[uint64]*cluster)
	clusters[known] = newCluster(members)

	for {
		select {
		case l := <-rg.lookupCh:
			heap.Push(rg.lookups, l)
		case ev := <-rg.evs:
			switch {
			case ev.Path == membersKey:
				switch ev.Type {
				case store.Add:
					path := ev.Path + "/" + ev.Body
					addr, _ := rg.st.LookupSync(path, ev.Seqn)
					members[ev.Body] = addr
				case store.Rem:
					members[ev.Body] = "", false
				}
			case ev.Type == store.Apply:
				known = ev.Seqn
				clusters[known] = newCluster(members)
			}
		}

		// If we have any lookups that can be satisfied, do them.
		for l := rg.lookups.peek(); known >= l.cver; l = rg.lookups.peek() {
			l := heap.Pop(rg.lookups).(lookup)
			l.ch <- clusters[l.cver]
		}
	}
}

func members(st *store.Store) map[string]string {
	members := map[string]string{}
	body, _ := st.Lookup(membersKey)
	ids := strings.Split(body, "\n", -1)
	for _, id := range ids {
		addr, _ := st.Lookup(membersKey + "/" + id)
		members[id] = addr
	}
	return members
}

func (rg *Registrar) clusterAt(cver uint64) *cluster {
	ch := make(chan *cluster)
	rg.lookupCh <- lookup{cver, ch}
	return <-ch
}

func (rg *Registrar) clusterFor(seqn uint64) *cluster {
	cver := uint64(1)
	if seqn > uint64(rg.window) {
		cver = seqn - uint64(rg.window)
	}
	return rg.clusterAt(cver)
}

func (rg *Registrar) History(a, b uint64) (res []map[string]string) {
	res = make([]map[string]string, b - a)
	for i := a; i < b; i++ {
		res[i-a] = rg.clusterAt(i).addrsById
	}
	return
}
