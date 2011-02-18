package consensus

import (
	"doozer/store"
	"sort"
)


type Run struct {
	Seqn  int64
	Cals  []string
	Addrs map[string]bool

	coordinator coordinator
	acceptor    acceptor
	learner     learner

	out chan packet
}


func (r *Run) Deliver(p packet) {
	m := r.coordinator.Deliver(p)
	if m != nil {
		r.out <- packet{M: *m}
	}

	m = r.acceptor.Put(&p.M)
	if m != nil {
		r.out <- packet{M: *m}
	}

	r.learner.Deliver(p)
}


func (r *Run) broadcast(m *M) {
	for addr := range r.Addrs {
		r.out <- packet{addr, *m}
	}
}


func GenerateRuns(alpha int64, w <-chan store.Event, runs chan<- *Run) {
	for e := range w {
		runs <- &Run{
			Seqn:  e.Seqn + alpha,
			Cals:  getCals(e),
			Addrs: getAddrs(e),
		}
	}
}

func getCals(g store.Getter) []string {
	slots := store.Getdir(g, "/doozer/slot")
	cals := make([]string, len(slots))

	for i, slot := range slots {
		cals[i] = store.GetString(g, "/doozer/slot/"+slot)
	}

	sort.SortStrings(cals)

	return cals
}


func getAddrs(g store.Getter) map[string]bool {
	// TODO include only CALs, once followers use TCP for updates.

	members := store.Getdir(g, "/doozer/info")
	addrs := make(map[string]bool)

	for _, member := range members {
		addrs[store.GetString(g, "/doozer/info/"+member+"/addr")] = true
	}

	return addrs
}
