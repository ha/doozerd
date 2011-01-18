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

type putCloser interface {
	Putter
	Close()
}

type ChanPutCloser chan *M

func (cp ChanPutCloser) Put(m *M) {
	go func() { cp <- m }()
}

func (cp ChanPutCloser) Close() {
	close(cp)
}

type putToWrapper struct {
	seqn int64
	pt   PutterTo
}

func (w putToWrapper) PutTo(m *M, addr string) {
	m.SetSeqn(w.seqn)
	w.pt.PutTo(m, addr)
}
