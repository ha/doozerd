package paxos

type Putter interface {
	Put(m Msg)
}

type putCloser interface {
	Putter
	Close()
}

type chanPutCloser chan Msg

func (cp chanPutCloser) Put(m Msg) {
	go func() { cp <- m }()
}

func (cp chanPutCloser) Close() {
	close(cp)
}

type putWrapper struct {
	seqn uint64
	Putter
}

func (w putWrapper) Put(m Msg) {
	m.SetSeqn(w.seqn)
	w.Putter.Put(m)
}
