package paxos

import (
	"log"
)

// TODO this is temporary during refactoring
const UNUSED = 0

type Instance struct {
	id     uint64
	nNodes uint64
	quorum uint64

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

// TODO this should take a Cluster as a param
func NewInstance(outs Putter, id, nNodes uint64, logger *log.Logger) *Instance {
	// TODO this ugly cast will go away when we take a Cluster param
	c := NewC(fakeCluster{outs, nNodes, int(id)})
	aIns, lIns := make(chan Msg), make(chan Msg)
	ins := &Instance{
		id: id,
		nNodes: nNodes,
		quorum: nNodes/2 + 1,
		vin: make(chan string),
		done: make(chan int),
		aIns:  aIns,
		lIns:  lIns,
		cPutter: c,
		aPutter: ChanPutter(aIns),
		lPutter: ChanPutter(lIns),
		logger: logger,
	}

	go acceptor(ins.aIns, outs)
	go func() {
		ins.v = learner(ins.quorum, ins.lIns)
		close(ins.done)
	}()

	return ins
}

func (ins *Instance) Put(m Msg) {
	if m.To == ins.id || m.To == 0 {
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


