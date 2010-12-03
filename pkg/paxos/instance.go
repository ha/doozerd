package paxos

import (
	"doozer/store"
	"rand"
	"time"
)

const initialBound = 1e6 // ns == 1ms

type instance chan Packet

type clusterer interface {
	cluster(seqn uint64) *cluster
}

func (it instance) process(seqn uint64, cf clusterer, res chan<- store.Op) {
	var sched bool
	var waitBound int64 = initialBound
	cx := cf.cluster(seqn)

	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: cx}
	ac := acceptor{outs: cx}
	ln := *newLearner(uint64(cx.Quorum()))
	var sk sink

	for p := range it {
		if p.Cmd() == tick {
			sched = false
		}

		p.SetFrom(cx.indexByAddr(p.Addr))
		co.Put(p.Msg)
		ac.Put(p.Msg)
		ln.Put(p.Msg)
		sk.Put(p.Msg)

		if co.seen > co.crnd && !sched {
			sched = true
			waitBound *= 2
			go func() {
				time.Sleep(rand.Int63n(waitBound))
				it.PutFrom("", msgTick)
			}()
		}

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
	// TODO try eliminating the goroutine
	go func() {
		it <- Packet{m, addr}
	}()
}

func (ins instance) Propose(v string) {
	// The from address doesn't matter.
	ins.PutFrom("", newPropose(v))
}
