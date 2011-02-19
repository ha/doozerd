package consensus

import (
	"doozer/store"
	"goprotobuf.googlecode.com/hg/proto"
	"sort"
)


type run struct {
	seqn  int64
	cals  []string
	addrs map[string]bool

	c coordinator
	a acceptor
	l learner

	out chan Packet
	ops chan<- store.Op
}


func (r *run) deliver(p packet) {
	m := r.c.deliver(p)
	r.broadcast(m)

	m = r.a.put(&p.M)
	r.broadcast(m)

	m, v, ok := r.l.deliver(p)
	r.broadcast(m)
	if ok {
		r.ops <- store.Op{r.seqn, string(v)}
	}
}


func (r *run) broadcast(m *M) {
	if m != nil {
		m.Seqn = &r.seqn
		b, _ := proto.Marshal(m)
		for addr := range r.addrs {
			r.out <- Packet{addr, b}
		}
	}
}


func GenerateRuns(alpha int64, w <-chan store.Event, runs chan<- *run) {
	for e := range w {
		runs <- &run{
			seqn:  e.Seqn + alpha,
			cals:  getCals(e),
			addrs: getAddrs(e),
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
