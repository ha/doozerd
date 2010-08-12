package paxos

type Putter interface {
	Put(m Msg)
}

type PutWrapper struct {
	seqn uint64
	Putter
}

func (w PutWrapper) Put(m Msg) {
	m.seqn = w.seqn
	w.Putter.Put(m)
}
