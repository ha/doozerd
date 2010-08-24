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
	cIns chan Msg
	cPutter Putter

	// Acceptor
	aIns chan Msg
	aPutter Putter

	// Learner
	lIns  chan Msg
	lPutter  Putter

	logger *log.Logger
}

func NewInstance(id, nNodes uint64, logger *log.Logger) *Instance {
	cIns, aIns, lIns := make(chan Msg), make(chan Msg), make(chan Msg)
	return &Instance{
		id: id,
		nNodes: nNodes,
		quorum: nNodes/2 + 1,
		vin: make(chan string),
		done: make(chan int),
		cIns:  cIns,
		aIns:  aIns,
		lIns:  lIns,
		cPutter: ChanPutter(cIns),
		aPutter: ChanPutter(aIns),
		lPutter: ChanPutter(lIns),
		logger: logger,
	}
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

func (ins *Instance) Init(outs Putter) {
	go coordinator(ins.id, UNUSED, ins.nNodes, ins.vin, ins.cIns, outs, make(chan int), ins.logger)
	go acceptor(ins.aIns, outs)
	go func() {
		ins.v = learner(ins.quorum, ins.lIns)
		close(ins.done)
	}()
}

func (ins *Instance) Close() {
	close(ins.cIns)
	close(ins.aIns)
	close(ins.lIns)
}

func (ins *Instance) Propose(v string) {
	ins.vin <- v
}


