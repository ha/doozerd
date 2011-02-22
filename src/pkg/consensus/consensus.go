package consensus

import (
	"doozer/store"
)


func NewManager(self string, alpha int64, in <-chan Packet, out chan<- Packet, ops chan<- store.Op, propSeqns chan<- int64, props <-chan *Prop, w <-chan store.Event) Manager {
	runs := make(chan *run)
	t := run{
		out:   out,
		ops:   ops,
		bound: initialWaitBound,
	}
	go generateRuns(alpha, w, runs, t)
	return newManager(self, propSeqns, in, runs, props)
}
