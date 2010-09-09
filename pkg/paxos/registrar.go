package paxos

import (
	"junta/store"
	"container/heap"
	"container/vector"
	"math"
	"path"
	"sort"
)

type Registrar struct {
	window   int
	st       *store.Store
	evs      chan store.Event
	lookupCh chan *lookup
	lookups  *lookupQueue
}

type lookup struct {
	cver    uint64
	ch      chan int
	members map[string]string
	actives []string
}

func (l *lookup) Less(y interface{}) bool {
	return l.cver < y.(*lookup).cver
}

type lookupQueue struct {
	vector.Vector
}

func (q *lookupQueue) peek() *lookup {
	if q.Len() == 0 {
		return &lookup{cver: math.MaxUint64} // ~infinity
	}
	return q.At(0).(*lookup)
}

// This thing keeps track of who is supposed to be in the cluster for every
// seqn. It also remembers the network address of every member.
// TODO remove the `start` param when store.Lookup provides a version
func NewRegistrar(st *store.Store, start uint64, window int) *Registrar {
	rg := &Registrar{
		window:   window,
		st:       st,
		evs:      make(chan store.Event),
		lookupCh: make(chan *lookup),
		lookups:  new(lookupQueue),
	}
	heap.Init(rg.lookups)
	st.Watch("**", rg.evs) // watch absolutely everything
	go rg.process(start, readdirMap(st, membersKey), readdirMap(st, slotKey))
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

func (rg *Registrar) process(known uint64, members, actives map[string]string) {
	clusters := make(map[uint64]map[string]string)
	copyMap(clusters, known, members)

	clusterActives := make(map[uint64][]string)
	copyActives(clusterActives, known, actives)

	for {
		select {
		case l := <-rg.lookupCh:
			heap.Push(rg.lookups, l)
		case ev := <-rg.evs:
			dir, name := path.Split(ev.Path)
			switch dir {
			case membersDir:
				members[name] = ev.Body, ev.IsSet()
			case slotDir:
				actives[name] = ev.Body, ev.IsSet()
			}
			known = ev.Seqn
			copyMap(clusters, known, members)
			copyActives(clusterActives, known, actives)
		}

		// If we have any lookups that can be satisfied, do them.
		for l := rg.lookups.peek(); known >= l.cver; l = rg.lookups.peek() {
			l := heap.Pop(rg.lookups).(*lookup)
			l.members = clusters[l.cver]
			l.actives = clusterActives[l.cver]
			l.ch <- 1
		}
	}
}

func copyMap(c map[uint64]map[string]string, n uint64, m map[string]string) {
	c[n] = map[string]string{}
	for k,v := range m {
		c[n][k] = v
	}
}

func copyActives(c map[uint64][]string, n uint64, a map[string]string) {
	c[n] = make([]string, len(a))
	i := 0
	for _,v := range a {
		if v != "" {
			c[n][i] = v
			i++
		}
	}
	c[n] = c[n][0:i]
	sort.SortStrings(c[n])
}

func readdirMap(st *store.Store, path string) map[string]string {
	m := map[string]string{}
	keys, cas := st.Lookup(path)
	if cas != store.Dir {
		return map[string]string{}
	}
	for _, key := range keys {
		v, cas := st.Lookup(path + "/" + key)
		if cas != store.Dir && cas != store.Missing {
			m[key] = v[0]
		}
	}
	return m
}

func (rg *Registrar) membersAt(cver uint64) (map[string]string, []string) {
	lk := lookup{cver:cver, ch:make(chan int)}
	rg.lookupCh <- &lk
	<-lk.ch
	return lk.members, lk.actives
}

func (rg *Registrar) membersFor(seqn uint64) (map[string]string, []string) {
	cver := uint64(1)
	if seqn > uint64(rg.window) {
		cver = seqn - uint64(rg.window)
	}
	return rg.membersAt(cver)
}
