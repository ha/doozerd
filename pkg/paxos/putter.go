package paxos

type Putter interface {
	Put(m Msg)
}

type PutCloser interface {
	Putter
	Close()
}

type PutCloseProcessor interface {
	PutCloser
	process(string)
}

type ChanPutCloser chan Msg

func (cp ChanPutCloser) Put(m Msg) {
	go func() { cp <- m }()
}

func (cp ChanPutCloser) Close() {
	close(cp)
}

type PutWrapper struct {
	seqn uint64
	from uint8
	Putter
}

func (w PutWrapper) Put(m Msg) {
	m.SetSeqn(w.seqn)
	m.SetFrom(w.from)
	w.Putter.Put(m)
}
