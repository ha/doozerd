package paxos

type instance struct {
	vin     chan string
	v       string
	done    chan int
	ins     chan Packet
	cPutter putCloser // Coordinator
	aPutter putCloser // Acceptor
	lPutter putCloser // Learner
	sPutter putCloser // Sink
	cx      *cluster
	cxReady chan int
}

func newInstance() *instance {
	cIns, aIns, lIns := make(ChanPutCloser), make(ChanPutCloser), make(ChanPutCloser)
	sIns := make(ChanPutCloser)
	ins := &instance{
		vin:     make(chan string),
		done:    make(chan int),
		ins:     make(chan Packet),
		cPutter: cIns,
		aPutter: aIns,
		lPutter: lIns,
		sPutter: sIns,
		cxReady: make(chan int),
	}

	go func() {
		<-ins.cxReady
		ch := make(chan string)
		go coordinator(cIns, ins.cx, ins.cx)
		go acceptor(aIns, ins.cx)
		go func() {
			ch <- learner(uint64(ins.cx.Quorum()), lIns)
		}()
		go func() {
			ch <- sink(sIns)
		}()

		for {
			select {
			case p := <-ins.ins:
				if closed(ins.ins) {
					ins.cPutter.Close()
					ins.aPutter.Close()
					ins.lPutter.Close()
					ins.sPutter.Close()
					return
				}
				p.SetFrom(ins.cx.indexByAddr(p.Addr))
				cIns.Put(p.Msg)
				aIns.Put(p.Msg)
				lIns.Put(p.Msg)
				sIns.Put(p.Msg)
			case v := <-ch:
				ins.v = v
				close(ch)
				close(ins.done)
				ins.cx.Put(newLearn(ins.v))
				return
			}
		}
	}()

	return ins
}

func (it *instance) setCluster(cx *cluster) {
	it.cx = cx
	close(it.cxReady)
}

func (it *instance) cluster() *cluster {
	<-it.cxReady
	return it.cx
}

func (it *instance) PutFrom(addr string, m Msg) {
	it.ins <- Packet{m, addr}
}

func (ins *instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *instance) Close() {
	close(ins.ins)
}

func (ins *instance) Propose(v string) {
	ins.cPutter.Put(newPropose(v))
}
