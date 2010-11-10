package paxos

import (
	"junta/store"
)

type instance chan Packet

type clusterer interface {
	cluster(seqn uint64) *cluster
}

func (it instance) process(seqn uint64, cf clusterer, res chan<- store.Op) {
	cx := cf.cluster(seqn)

	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: cx}
	ac := acceptor{outs: cx}
	ln := *newLearner(uint64(cx.Quorum()))
	var sk sink

	for p := range it {
		p.SetFrom(cx.indexByAddr(p.Addr))
		co.Put(p.Msg)
		ac.Put(p.Msg)
		ln.Put(p.Msg)
		sk.Put(p.Msg)

		if sk.done {
			cx.Put(newLearn(sk.v))
			res <- store.Op{seqn, sk.v}
			return
		}
		if ln.done {
			cx.Put(newLearn(ln.v))
			res <- store.Op{seqn, ln.v}
			return
		}
	}
}

func (it instance) PutFrom(addr string, m Msg) {
	go func() {
		it <- Packet{m, addr}
	}()
}

func (ins instance) Propose(v string) {
	// The from address doesn't matter.
	ins.PutFrom("", newPropose(v))
}
