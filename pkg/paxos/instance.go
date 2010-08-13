package paxos

type Instance struct {
	id     uint64
	quorum uint64

	vin  chan string
	v    string
	done chan int


	// Coordinator
	cIns chan Msg
	cPutter Putter

	// Acceptor
	aIns chan Msg
	aPutter Putter

	// Learner
	lIns  chan Msg
	lPutter  Putter
}

func NewInstance(id, quorum uint64) *Instance {
	cIns, aIns, lIns := make(chan Msg), make(chan Msg), make(chan Msg)
	return &Instance{
		id: id,
		quorum: quorum,
		vin: make(chan string),
		done: make(chan int),
		cIns:  cIns,
		aIns:  aIns,
		lIns:  lIns,
		cPutter: ChanPutter(cIns),
		aPutter: ChanPutter(aIns),
		lPutter: ChanPutter(lIns),
	}
}

func (ins *Instance) Put(m Msg) {
	if m.to == ins.id || m.to == 0 {
		ins.cPutter.Put(m)
		ins.aPutter.Put(m)
		ins.lPutter.Put(m)
	}
}

func (ins *Instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *Instance) Init(outs Putter) {
	go coordinator(ins.id, ins.quorum, 3, ins.vin, ins.cIns, outs, make(chan int))
	go acceptor(ins.aIns, outs)
	go func() {
		ins.v = learner(ins.quorum, ins.lIns)
		close(ins.done)
	}()
}

func (ins *Instance) Close() {
	close(ins.cIns)
	close(ins.aIns)
	close(ins.lIns)
}

func (ins *Instance) Propose(v string) {
	ins.vin <- v
}


