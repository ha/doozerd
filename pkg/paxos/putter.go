package paxos

type Putter interface {
	Put(m Msg)
}

type ChanPutter chan Msg

func (cp ChanPutter) Put(m Msg) {
	go func() { cp <- m }()
}

type PutWrapper struct {
	seqn, from uint64
	Putter
}

func (w PutWrapper) Put(m Msg) {
	m.seqn = w.seqn
	m.from = w.from
	w.Putter.Put(m)
}
