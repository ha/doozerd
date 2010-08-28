package paxos

import (
	"log"
)

type instance struct {
	vin  chan string
	v    string
	done chan int

	// Coordinator
	cPutter putCloser

	// Acceptor
	aPutter putCloser

	// Learner
	lPutter putCloser

	logger *log.Logger
}

func newInstance(cx *cluster, outs Putter, logger *log.Logger) *instance {
	c := newCoord(outs)
	aIns, lIns := make(chan Msg), make(chan Msg)
	ins := &instance{
		vin:     make(chan string),
		done:    make(chan int),
		cPutter: c,
		aPutter: chanPutCloser(aIns),
		lPutter: chanPutCloser(lIns),
		logger:  logger,
	}

	go c.process(cx)
	go acceptor(aIns, outs)
	go func() {
		ins.v = learner(uint64(cx.Quorum()), lIns)
		close(ins.done)
	}()

	return ins
}

func (ins *instance) Put(m Msg) {
	ins.cPutter.Put(m)
	ins.aPutter.Put(m)
	ins.lPutter.Put(m)
}

func (ins *instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *instance) Close() {
	ins.cPutter.Close()
	ins.aPutter.Close()
	ins.lPutter.Close()
}

func (ins *instance) Propose(v string) {
	ins.cPutter.Put(newPropose(v))
}
