package paxos

type Putter interface {
	Put(m Msg)
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
