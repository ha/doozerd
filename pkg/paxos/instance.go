package paxos

type instance struct {
	ins     chan Packet
}

type clusterer interface {
	cluster(seqn uint64) *cluster
}

func newInstance(seqn uint64, cf clusterer, res chan result) *instance {
	cIns, lIns := make(ChanPutCloser), make(ChanPutCloser)
	ins := &instance{
		ins:     make(chan Packet),
	}

	go func() {
		cx := cf.cluster(seqn)

		ch := make(chan string)
		ac := acceptor{outs:cx}
		var sk sink

		go coordinator(cIns, cx, cx)
		go func() {
			ch <- learner(uint64(cx.Quorum()), lIns)
		}()

		for {
			select {
			case p := <-ins.ins:
				if closed(ins.ins) {
					cIns.Close()
					lIns.Close()
					return
				}
				p.SetFrom(cx.indexByAddr(p.Addr))
				cIns.Put(p.Msg)
				ac.Put(p.Msg)
				lIns.Put(p.Msg)
				sk.Put(p.Msg)

				if sk.done {
					close(ch)
					cx.Put(newLearn(sk.v))
					res <- result{seqn, sk.v}
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
