package consensus

import (
	"doozer/store"
	"time"
)


// propSeqns must be buffered with capacity >= alpha
// defRev is the rev in which this manager was defined;
// it will participate starting at defRev+alpha
func NewManager(self string, defRev int64, alpha int64, in <-chan Packet, out chan<- Packet, ops chan<- store.Op, propSeqns chan<- int64, props <-chan *Prop, fillDelay int64, st *store.Store) Manager {
	runs := make(chan *run)
	t := run{
		self:  self,
		out:   out,
		ops:   ops,
		bound: initialWaitBound,
	}
	go generateRuns(defRev, alpha, st, runs, t)
	return newManager(self, defRev+alpha, propSeqns, in, runs, props, time.Tick(10e6), fillDelay, st, out)
}


type Proposer interface {
	Propose(v []byte) store.Event
}


func Set(p Proposer, path string, body []byte, rev int64) (e store.Event) {
	e.Mut, e.Err = store.EncodeSet(path, string(body), rev)
	if e.Err != nil {
		return
	}

	return p.Propose([]byte(e.Mut))
}


func Del(p Proposer, path string, rev int64) (e store.Event) {
	e.Mut, e.Err = store.EncodeDel(path, rev)
	if e.Err != nil {
		return
	}

	return p.Propose([]byte(e.Mut))
}
