package paxos

type Packet struct {
	*M
	Addr string
}

type Putter interface {
	Put(m *M)
}

type PutterTo interface {
	PutTo(m *M, addr string)
}

type putToWrapper struct {
	seqn int64
	pt   PutterTo
}

func (w putToWrapper) PutTo(m *M, addr string) {
	m.SetSeqn(w.seqn)
	w.pt.PutTo(m, addr)
}
