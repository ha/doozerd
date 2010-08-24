package paxos

import (
	"log"
)

// TODO this is temporary during refactoring
const UNUSED = 0

type Instance struct {
	cx Cluster

	vin  chan string
	v    string
	done chan int


	// Coordinator
	cPutter PutCloseProcessor

	// Acceptor
	aIns chan Msg
	aPutter Putter

	// Learner
	lIns  chan Msg
	lPutter  Putter

	logger *log.Logger
}

func NewInstance(cx Cluster, logger *log.Logger) *Instance {
	c := NewC(cx)
	aIns, lIns := make(chan Msg), make(chan Msg)
	ins := &Instance{
		cx: cx,
		vin: make(chan string),
		done: make(chan int),
		aIns:  aIns,
		lIns:  lIns,
		cPutter: c,
		aPutter: ChanPutter(aIns),
		lPutter: ChanPutter(lIns),
		logger: logger,
	}

	go acceptor(ins.aIns, cx)
	go func() {
		ins.v = learner(uint64(cx.Quorum()), ins.lIns)
		close(ins.done)
	}()

	return ins
}

func (ins *Instance) Put(m Msg) {
	if m.To == uint64(ins.cx.SelfIndex()) || m.To == 0 {
		ins.cPutter.Put(m)
		ins.aPutter.Put(m)
		ins.lPutter.Put(m)
	}
}

func (ins *Instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *Instance) Close() {
	ins.cPutter.Close()
	close(ins.aIns)
	close(ins.lIns)
}

func (ins *Instance) Propose(v string) {
	// TODO make propose into a message type. This becomes:
	//   ins.cPutter.Put(...)
	go ins.cPutter.process(v)
}


