package paxos

import (
	"junta/store"
	"container/heap"
	"container/vector"
	"math"
	"path"
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
	ch   chan map[string]string
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
// TODO remove the `start` param when store.Lookup provides a version
func NewRegistrar(st *store.Store, start uint64, window int) *Registrar {
	rg := &Registrar{
		window:   window,
		st:       st,
		evs:      make(chan store.Event),
		lookupCh: make(chan lookup),
		lookups:  new(lookupQueue),
	}
	heap.Init(rg.lookups)
	st.Watch("**", rg.evs) // watch absolutely everything
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
	clusters := make(map[uint64]map[string]string)
	copyMap(clusters, known, members)

	for {
		select {
		case l := <-rg.lookupCh:
			heap.Push(rg.lookups, l)
		case ev := <-rg.evs:
			dir, name := path.Split(ev.Path)
			switch dir {
			case membersDir:
				members[name] = ev.Body, ev.IsSet()
			}
			known = ev.Seqn
			copyMap(clusters, known, members)
		}

		// If we have any lookups that can be satisfied, do them.
		for l := rg.lookups.peek(); known >= l.cver; l = rg.lookups.peek() {
			l := heap.Pop(rg.lookups).(lookup)
			l.ch <- clusters[l.cver]
		}
	}
}

func copyMap(c map[uint64]map[string]string, n uint64, m map[string]string) {
	c[n] = map[string]string{}
	for k,v := range m {
		c[n][k] = v
	}
}

func members(st *store.Store) map[string]string {
	members := map[string]string{}
	ids, _ := st.Lookup(membersKey)
	for _, id := range ids {
		if id != "" {
			addr, _ := st.Lookup(membersDir + id)
			members[id] = addr[0]
		}
	}
	return members
}

func (rg *Registrar) membersAt(cver uint64) (map[string]string, []string) {
	ch := make(chan map[string]string)
	rg.lookupCh <- lookup{cver, ch}
	ms := <-ch
	return ms, stringKeys(ms)
}

func (rg *Registrar) membersFor(seqn uint64) (map[string]string, []string) {
	cver := uint64(1)
	if seqn > uint64(rg.window) {
		cver = seqn - uint64(rg.window)
	}
	return rg.membersAt(cver)
}
