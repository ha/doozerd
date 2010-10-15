package paxos

type instance struct {
	vin     chan string
	v       string
	done    chan int
	ins     chan Packet
	cPutter putCloser // Coordinator
}

type clusterer interface {
	cluster(seqn uint64) *cluster
}

func newInstance(seqn uint64, cf clusterer) *instance {
	cIns, aIns, lIns := make(ChanPutCloser), make(ChanPutCloser), make(ChanPutCloser)
	sIns := make(ChanPutCloser)
	ins := &instance{
		vin:     make(chan string),
		done:    make(chan int),
		ins:     make(chan Packet),
		cPutter: cIns,
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
					ins.cPutter.Close()
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
				ins.v = v
				close(ch)
				close(ins.done)
				cx.Put(newLearn(ins.v))
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

func (ins *instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *instance) Close() {
	close(ins.ins)
}

func (ins *instance) Propose(v string) {
	// The from address doesn't matter.
	ins.PutFrom("", newPropose(v))
}
