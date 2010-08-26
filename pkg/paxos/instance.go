package paxos

import (
	"log"
)

type Instance struct {
	cx Cluster

	vin  chan string
	v    string
	done chan int


	// Coordinator
	cPutter PutCloseProcessor

	// Acceptor
	aPutter PutCloser

	// Learner
	lPutter  PutCloser

	logger *log.Logger
}

func NewInstance(cx Cluster, logger *log.Logger) *Instance {
	c := NewC(cx)
	aIns, lIns := make(chan Message), make(chan Message)
	ins := &Instance{
		cx: cx,
		vin: make(chan string),
		done: make(chan int),
		cPutter: c,
		aPutter: ChanPutCloser(aIns),
		lPutter: ChanPutCloser(lIns),
		logger: logger,
	}

	go acceptor(aIns, cx)
	go func() {
		ins.v = learner(uint64(cx.Quorum()), lIns)
		close(ins.done)
	}()

	return ins
}

func (ins *Instance) Put(m Message) {
	ins.cPutter.Put(m)
	ins.aPutter.Put(m)
	ins.lPutter.Put(m)
}

func (ins *Instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *Instance) Close() {
	ins.cPutter.Close()
	ins.aPutter.Close()
	ins.lPutter.Close()
}

func (ins *Instance) Propose(v string) {
	// TODO make propose into a message type. This becomes:
	//   ins.cPutter.Put(...)
	go ins.cPutter.process(v)
}
