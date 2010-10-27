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
		cx := cf.cluster(seqn)

		ch := make(chan string)
		ac := acceptor{outs:cx}
		ln := *newLearner(uint64(cx.Quorum()))
		var sk sink

		go coordinator(cIns, cx, cx)

		for {
			select {
			case p := <-ins.ins:
				if closed(ins.ins) {
					cIns.Close()
					return
				}
				p.SetFrom(cx.indexByAddr(p.Addr))
				cIns.Put(p.Msg)
				ac.Put(p.Msg)
				ln.Put(p.Msg)
				sk.Put(p.Msg)

				if sk.done {
					close(ch)
					cx.Put(newLearn(sk.v))
					res <- result{seqn, sk.v}
					return
				}
				if ln.done {
					close(ch)
					cx.Put(newLearn(ln.v))
					res <- result{seqn, ln.v}
					return
				}
			case v := <-ch:
				close(ch)
				cx.Put(newLearn(v))
				res <- result{seqn, v}
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
