package paxos

type Putter interface {
	Put(m Msg)
}

type putCloser interface {
	Putter
	Close()
}

type putCloseProcessor interface {
	putCloser
	process(string)
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
	from uint8
	Putter
}

func (w putWrapper) Put(m Msg) {
	m.SetSeqn(w.seqn)
	m.SetFrom(w.from)
	w.Putter.Put(m)
}
