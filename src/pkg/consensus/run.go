package consensus

import (
	"doozer/store"
	"goprotobuf.googlecode.com/hg/proto"
	"rand"
	"sort"
	"time"
)


const initialWaitBound = 1e6 // ns == 1ms


type run struct {
	seqn  int64
	self  string
	cals  []string
	addrs map[string]bool

	c coordinator
	a acceptor
	l learner

	out   chan<- Packet
	ops   chan<- store.Op
	ticks chan<- int64
	bound int64
}


func (r *run) quorum() int {
	return len(r.cals)/2 + 1
}


func (r *run) deliver(p packet) {
	m, tick := r.c.deliver(p)
	r.broadcast(m)
	if tick {
		r.bound *= 2
		go func() {
			time.Sleep(rand.Int63n(r.bound))
			r.ticks <- r.seqn
		}()
	}

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


func (r *run) indexOf(self string) int64 {
	for i, id := range r.cals {
		if id == self {
			return int64(i)
		}
	}
	return -1
}


func (r *run) isLeader(self string) bool {
	for i, id := range r.cals {
		if id == self {
			return r.seqn%int64(len(r.cals)) == int64(i)
		}
	}
	return false
}


func generateRuns(alpha int64, w <-chan store.Event, runs chan<- *run, t run) {
	for e := range w {
		r := t
		r.seqn = e.Seqn + alpha
		r.cals = getCals(e)
		r.addrs = getAddrs(e)
		r.c.size = len(r.cals)
		r.c.quor = r.quorum()
		r.c.crnd = r.indexOf(r.self) + int64(len(r.cals))
		r.l.init(int64(r.quorum()))
		runs <- &r
	}
}

func getCals(g store.Getter) []string {
	slots := store.Getdir(g, "/doozer/slot")
	cals := make([]string, len(slots))

	i := 0
	for _, slot := range slots {
		id := store.GetString(g, "/doozer/slot/"+slot)
		if id != "" {
			cals[i] = id
			i++
		}
	}

	cals = cals[0:i]
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
