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
	m.Seqn = w.seqn
	m.From = w.from
	w.Putter.Put(m)
}
