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
	alpha    int
	st       *store.Store
	evs      chan store.Event
	lookupCh chan *lookup
	lookups  *lookupQueue
}

type lookup struct {
	cver      uint64
	done      chan int
	memberSet map[string]string
	calSet    []string
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
// TODO remove the `start` param when store.Get provides a version
func NewRegistrar(st *store.Store, start uint64, alpha int) *Registrar {
	rg := &Registrar{
		alpha:    alpha,
		st:       st,
		evs:      make(chan store.Event),
		lookupCh: make(chan *lookup),
		lookups:  new(lookupQueue),
	}
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

func (rg *Registrar) process(seqn uint64, memberSet, calSet map[string]string) {
	memberSets := make(map[uint64]map[string]string)
	memberSets[seqn] = dup(memberSet)

	calSets := make(map[uint64][]string)
	calSets[seqn] = nonEmpty(values(calSet))
	sort.SortStrings(calSets[seqn])

	for {
		select {
		case l := <-rg.lookupCh:
			heap.Push(rg.lookups, l)
		case ev := <-rg.evs:
			dir, name := path.Split(ev.Path)
			switch dir {
			case membersDir:
				memberSet[name] = ev.Body, ev.IsSet()
			case slotDir:
				calSet[name] = ev.Body, ev.IsSet()
			}
			seqn = ev.Seqn
			memberSets[seqn] = dup(memberSet)
			calSets[seqn] = nonEmpty(values(calSet))
			sort.SortStrings(calSets[seqn])
		}

		// If we have any lookups that can be satisfied, do them.
		for l := rg.lookups.peek(); seqn >= l.cver; l = rg.lookups.peek() {
			heap.Pop(rg.lookups)
			l.memberSet = memberSets[l.cver]
			l.calSet = calSets[l.cver]
			l.done <- 1
		}
	}
}

func dup(m map[string]string) map[string]string {
	o := map[string]string{}
	for k, v := range m {
		o[k] = v
	}
	return o
}

func values(m map[string]string) []string {
	i, o := 0, make([]string, len(m))
	for _, v := range m {
		o[i] = v
		i++
	}
	return o[0:i]
}

func nonEmpty(s []string) []string {
	i, o := 0, make([]string, len(s))
	for _, v := range s {
		if v != "" {
			o[i] = v
			i++
		}
	}
	return o[0:i]
}

func readdirMap(st *store.Store, path string) map[string]string {
	m := map[string]string{}
	keys, cas := st.Get(path)
	if cas != store.Dir {
		return map[string]string{}
	}
	for _, key := range keys {
		v, cas := st.Get(path + "/" + key)
		if cas != store.Dir && cas != store.Missing {
			m[key] = v[0]
		}
	}
	return m
}

func (rg *Registrar) setsForVersion(cver uint64) (map[string]string, []string) {
	lk := lookup{cver: cver, done: make(chan int)}
	rg.lookupCh <- &lk
	<-lk.done
	return lk.memberSet, lk.calSet
}

func (rg *Registrar) setsForSeqn(seqn uint64) (map[string]string, []string) {
	cver := uint64(1)
	if seqn > uint64(rg.alpha) {
		cver = seqn - uint64(rg.alpha)
	}
	return rg.setsForVersion(cver)
}
