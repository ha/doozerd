package paxos

type instance struct {
	ins     chan Packet
}

type clusterer interface {
	cluster(seqn uint64) *cluster
}

func newInstance(seqn uint64, cf clusterer, res chan result) *instance {
	cIns, aIns, lIns := make(ChanPutCloser), make(ChanPutCloser), make(ChanPutCloser)
	sIns := make(ChanPutCloser)
	ins := &instance{
		ins:     make(chan Packet),
	}

	go func() {
		cx := cf.cluster(seqn)

		ch := make(chan string)
		go coordinator(cIns, cx, cx)
		go acceptor(aIns, cx)
		go func() {
			ch <- learner(uint64(cx.Quorum()), lIns)
		}()
		go func() {
			ch <- sink(sIns)
		}()

		for {
			select {
			case p := <-ins.ins:
				if closed(ins.ins) {
					cIns.Close()
					aIns.Close()
					lIns.Close()
					sIns.Close()
					return
				}
				p.SetFrom(cx.indexByAddr(p.Addr))
				cIns.Put(p.Msg)
				aIns.Put(p.Msg)
				lIns.Put(p.Msg)
				sIns.Put(p.Msg)
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
