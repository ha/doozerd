package paxos

import (
	"doozer/store"
	"rand"
	"time"
)

const initialBound = 1e6 // ns == 1ms

type instance chan Packet

type clusterer interface {
	cluster(seqn int64) *cluster
}

func (it instance) process(seqn int64, cf clusterer, res chan<- store.Op) {
	var sched bool
	var waitBound int64 = initialBound
	cx := cf.cluster(seqn)

	co := coordinator{cx: cx, crnd: int64(cx.SelfIndex()), outs: cx}
	ac := acceptor{outs: cx}
	ln := *newLearner(int64(cx.Quorum()))
	var sk sink

	for p := range it {
		if p.Cmd() == M_TICK {
			sched = false
		}

		p.SetFrom(int32(cx.indexByAddr(p.Addr)))
		co.Put(p.M)
		ac.Put(p.M)
		lnOk := ln.Put(p.M)
		skOk := sk.Put(p.M)

		if co.seen > co.crnd && !sched {
			sched = true
			waitBound *= 2
			go func() {
				time.Sleep(rand.Int63n(waitBound))
				it.PutFrom("", msgTick)
			}()
		}

		if skOk {
			res <- store.Op{seqn, sk.v}
		}
		if lnOk {
			cx.Put(&M{WireCmd: learn, Value: []byte(ln.v)})
			res <- store.Op{seqn, ln.v}
		}
	}
}

func (it instance) PutFrom(addr string, m *M) {
	// TODO try eliminating the goroutine
	go func() {
		it <- Packet{m, addr}
	}()
}

func (ins instance) Propose(v string) {
	// The from address doesn't matter.
	ins.PutFrom("", &M{WireCmd: propose, Value: []byte(v)})
}
