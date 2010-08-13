package paxos

type Instance struct {
	quorum uint64

	vin  chan string
	v    string
	done chan int


	// Coordinator
	cIns chan Msg

	// Acceptor
	aIns chan Msg

	// Learner
	lIns  chan Msg
}

func NewInstance(quorum uint64) *Instance {
	return &Instance{
		quorum: quorum,
		vin: make(chan string),
		done: make(chan int),
		cIns:  make(chan Msg),
		aIns:  make(chan Msg),
		lIns:  make(chan Msg),
	}
}

func (ins *Instance) Put(m Msg) {
	go func() { ins.cIns <- m }()
	go func() { ins.aIns <- m }()
	go func() { ins.lIns <- m }()
}

func (ins *Instance) Value() string {
	<-ins.done
	return ins.v
}

func (ins *Instance) Init(outs Putter) {
	go coordinator(1, ins.quorum, 3, ins.vin, ins.cIns, outs, make(chan int))
	go acceptor(2, ins.aIns, outs)
	go func() {
		ins.v = learner(1, ins.lIns)
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


