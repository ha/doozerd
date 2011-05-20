package consensus

import (
	"container/heap"
	"doozer/store"
	"goprotobuf.googlecode.com/hg/proto"
	"log"
	"rand"
	"time"
)

const initialWaitBound = 1e6 // ns == 1ms


type run struct {
	seqn int64
	self string
	cals []string
	addr []string

	c coordinator
	a acceptor
	l learner

	out   chan<- Packet
	ops   chan<- store.Op
	bound int64
	ntick int
	prop  bool
}


func (r *run) quorum() int {
	return len(r.cals)/2 + 1
}


func (r *run) update(p packet, ticks heap.Interface) {
	if p.msg.Cmd != nil && *p.msg.Cmd == msg_TICK {
		log.Printf("tick wasteful=%v", r.l.done)
	}

	m, tick := r.c.update(p)
	r.broadcast(m)
	if tick {
		r.ntick++
		r.bound *= 2
		t := rand.Int63n(r.bound)
		log.Printf("sched tick=%d seqn=%d t=%d", r.ntick, r.seqn, t)
		schedTrigger(ticks, r.seqn, time.Nanoseconds(), t)
	}

	m = r.a.update(&p.msg)
	r.broadcast(m)

	m, v, ok := r.l.update(p)
	r.broadcast(m)
	if ok {
		log.Printf("learn seqn=%d", r.seqn)
		r.ops <- store.Op{r.seqn, string(v)}
	}
}


func (r *run) broadcast(m *msg) {
	if m != nil {
		m.Seqn = &r.seqn
		b, _ := proto.Marshal(m)
		for _, addr := range r.addr {
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
