package consensus

import (
	"doozer/store"
	"time"
)


// propSeqns must be buffered with capacity >= alpha
func NewManager(self string, start int64, alpha int64, in <-chan Packet, out chan<- Packet, ops chan<- store.Op, propSeqns chan<- int64, props <-chan *Prop, w <-chan store.Event, fillDelay int64, st *store.Store) Manager {
	runs := make(chan *run)
	t := run{
		self:  self,
		out:   out,
		ops:   ops,
		bound: initialWaitBound,
	}
	go generateRuns(alpha, w, runs, t)
	return newManager(self, start, propSeqns, in, runs, props, time.Tick(10e6), fillDelay, st, out)
}


type Proposer interface {
	Propose(v []byte) store.Event
}


func Set(p Proposer, path string, body []byte, cas int64) (e store.Event) {
	e.Mut, e.Err = store.EncodeSet(path, string(body), cas)
	if e.Err != nil {
		return
	}

	return p.Propose([]byte(e.Mut))
}


func Del(p Proposer, path string, cas int64) (e store.Event) {
	e.Mut, e.Err = store.EncodeDel(path, cas)
	if e.Err != nil {
		return
	}

	return p.Propose([]byte(e.Mut))
}
