package consensus

import (
	"container/heap"
	"doozer/store"
	"goprotobuf.googlecode.com/hg/proto"
	"log"
	"rand"
	"time"
)

const initialWaitBound = 1e8


type run struct {
	seqn int64
	self string
	cals []string
	addr []string
	cfg  int64 // seqn of configuration

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
		r.tick(ticks)
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


func (r *run) multiPropose(v []byte, ticks heap.Interface) {
	if r != nil {
		m := r.c.multiPropose(v)
		r.broadcast(m)
		r.tick(ticks)
	}
}


func (r *run) tick(q heap.Interface) {
	r.ntick++
	r.bound *= 2
	t := rand.Int63n(r.bound) //+ tfill
	log.Printf("sched tick=%d seqn=%d t=%d", r.ntick, r.seqn, t)
	schedTrigger(q, r.seqn, time.Nanoseconds(), t)
}


func (r *run) broadcast(m *msg) {
	if m != nil {
		m.Seqn = &r.seqn
		b, _ := proto.Marshal(m)
		for _, addr := range r.addr {
			log.Println("sending packet to", addr)
			r.out <- Packet{addr, b}
		}
	}
}


func (r *run) nextLeaderSeqn(id string) int64 {
	k := int64(len(r.cals))
	return r.seqn + k - r.seqn%k + r.iId(id)
}


func (r *run) iLeader() int64 {
	return r.seqn % int64(len(r.cals))
}


func (r *run) iId(id string) int64 {
	for i, s := range r.cals {
		if s == id {
			return int64(i)
		}
	}
	return -1
}


func (r *run) iAddr(addr string) int64 {
	for i, s := range r.addr {
		if s == addr {
			return int64(i)
		}
	}
	return -1
}


func (r *run) eqCals(o *run) bool {
	if o == nil || len(r.cals) != len(o.cals) {
		return false
	}
	for i := range r.cals {
		if r.cals[i] != o.cals[i] {
			return false
		}
	}
	return true
}
