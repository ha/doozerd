package consensus

import (
	"container/heap"
	"doozer/store"
	"goprotobuf.googlecode.com/hg/proto"
	"rand"
	"sort"
	"log"
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
	bound int64
}


func (r *run) quorum() int {
	return len(r.cals)/2 + 1
}


func (r *run) update(p packet, ticks heap.Interface) (learned bool) {
	if p.msg.Cmd != nil && *p.msg.Cmd == msg_TICK {
		log.Printf("tick wasteful=%v", r.l.done)
	}

	if r.l.done {
		return false
	}

	m, tick := r.c.update(p)
	r.broadcast(m)
	if tick {
		r.bound *= 2
		schedTrigger(ticks, r.seqn, rand.Int63n(r.bound))
	}

	m = r.a.update(&p.msg)
	r.broadcast(m)

	m, v, ok := r.l.update(p)
	r.broadcast(m)
	if ok {
		log.Printf("learn seqn=%d", r.seqn)
		r.ops <- store.Op{r.seqn, string(v)}
		return true
	}

	return false
}


func (r *run) broadcast(m *msg) {
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
	ents := store.Getdir(g, "/ctl/cal")
	cals := make([]string, len(ents))

	i := 0
	for _, cal := range ents {
		id := store.GetString(g, "/ctl/cal/"+cal)
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

	ids := store.Getdir(g, "/ctl/node")
	addrs := make(map[string]bool)

	for _, id := range ids {
		addrs[store.GetString(g, "/ctl/node/"+id+"/addr")] = true
	}

	return addrs
}
