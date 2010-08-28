package paxos

import (
	"borg/store"
	"log"
	"strings"
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

func newInstance(self string, st *store.Store, cver uint64, outs Putter, logger *log.Logger) *instance {
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

	go func() {
		nodes, ok := st.LookupSync("/b/borg/members", cver)
		if !ok {
			// No members? We are seriously F'd in the A.
			logger.Log("no members")
			panic("no members")
		}
		cx := newCluster(self, strings.Split(nodes, "\n", -1))

		go c.process(cx)
		go acceptor(aIns, outs)
		go func() {
			ins.v = learner(uint64(cx.Quorum()), lIns)
			close(ins.done)
		}()
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
