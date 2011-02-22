package consensus

import (
	"doozer/store"
)


func NewManager(self string, alpha int64, in <-chan Packet, out chan<- Packet, ops chan<- store.Op, propSeqns chan<- int64, props <-chan *Prop, w <-chan store.Event) Manager {
	runs := make(chan *run)
	t := run{
		self:  self,
		out:   out,
		ops:   ops,
		bound: initialWaitBound,
	}
	go generateRuns(alpha, w, runs, t)
	return newManager(self, propSeqns, in, runs, props)
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
