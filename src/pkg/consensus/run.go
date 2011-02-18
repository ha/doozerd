package consensus

import (
	"doozer/store"
	"sort"
)


type Run struct {
	Seqn int64
	Cals []string

	coordinator coordinator
	acceptor    acceptor
	learner     learner

	out chan Packet
}


func (r *Run) Deliver(p Packet) {
	m := r.coordinator.Deliver(p)
	if m != nil {
		r.out <- Packet{M: *m}
	}

	m = r.acceptor.Put(&p.M)
	if m != nil {
		r.out <- Packet{M: *m}
	}

	r.learner.Deliver(p)
}


func GenerateRuns(alpha int64, w <-chan store.Event, runs chan<- Run) {
	for e := range w {
		runs <- Run{Seqn: e.Seqn + alpha, Cals: getCals(e)}
	}
}

func getCals(g store.Getter) []string {
	slots := store.Getdir(g, "/doozer/slot")
	cals  := make([]string, len(slots))

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
