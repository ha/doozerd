package paxos

type instance struct {
	ins     chan Packet
}

type clusterer interface {
	cluster(seqn uint64) *cluster
}

func newInstance(seqn uint64, cf clusterer, res chan result) *instance {
	cIns := make(ChanPutCloser)
	ins := &instance{
		ins:     make(chan Packet),
	}

	go func() {
		defer cIns.Close()

		cx := cf.cluster(seqn)

		ac := acceptor{outs:cx}
		ln := *newLearner(uint64(cx.Quorum()))
		var sk sink

		go coordinator(cIns, cx, cx)

		for p := range ins.ins {
			p.SetFrom(cx.indexByAddr(p.Addr))
			cIns.Put(p.Msg)
			ac.Put(p.Msg)
			ln.Put(p.Msg)
			sk.Put(p.Msg)

			if sk.done {
				cx.Put(newLearn(sk.v))
				res <- result{seqn, sk.v}
				return
			}
			if ln.done {
				cx.Put(newLearn(ln.v))
				res <- result{seqn, ln.v}
				return
			}
		}
	}()

	return ins
}

func (it *instance) PutFrom(addr string, m Msg) {
	go func() {
		it.ins <- Packet{m, addr}
	}()
}

func (ins *instance) Close() {
	close(ins.ins)
}

func (ins *instance) Propose(v string) {
	// The from address doesn't matter.
	ins.PutFrom("", newPropose(v))
}
