package paxos

type instance struct {
	vin     chan string
	v       string
	done    chan int
	cPutter putCloser // Coordinator
	aPutter putCloser // Acceptor
	lPutter putCloser // Learner
	sPutter putCloser // Sink
	cx      *cluster
	cxReady chan int
}

func newInstance(cxf func() *cluster) *instance {
	cIns, aIns, lIns := make(ChanPutCloser), make(ChanPutCloser), make(ChanPutCloser)
	sIns := make(ChanPutCloser)
	ins := &instance{
		vin:     make(chan string),
		done:    make(chan int),
		cPutter: cIns,
		aPutter: aIns,
		lPutter: lIns,
		sPutter: sIns,
		cxReady: make(chan int),
	}

	go func() {
		ch := make(chan string)
		ins.cx = cxf()
		close(ins.cxReady)
		go coordinator(cIns, ins.cx, ins.cx)
		go acceptor(aIns, ins.cx)
		go func() {
			ch <- learner(uint64(ins.cx.Quorum()), lIns)
		}()
		go func() {
			ch <- sink(sIns)
		}()

		ins.v = <-ch
		close(ch)
		close(ins.done)
		ins.cx.Put(newLearn(ins.v))
	}()

	return ins
}

func (it *instance) cluster() *cluster {
	<-it.cxReady
	return it.cx
}

func (ins *instance) Put(m Msg) {
	ins.cPutter.Put(m)
	ins.aPutter.Put(m)
	ins.lPutter.Put(m)
	ins.sPutter.Put(m)
}

func (ins *instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *instance) Close() {
	ins.cPutter.Close()
	ins.aPutter.Close()
	ins.lPutter.Close()
	ins.sPutter.Close()
}

func (ins *instance) Propose(v string) {
	ins.cPutter.Put(newPropose(v))
}
