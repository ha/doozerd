package paxos

type Putter interface {
	Put(m Message)
}

type PutCloser interface {
	Putter
	Close()
}

type PutCloseProcessor interface {
	PutCloser
	process(string)
}

type ChanPutter chan Message

func (cp ChanPutter) Put(m Message) {
	go func() { cp <- m }()
}

type PutWrapper struct {
	seqn, from uint64
	Putter
}

func (w PutWrapper) Put(m Message) {
	m.SetSeqn(w.seqn)
	m.SetFrom(byte(w.from))
	w.Putter.Put(m)
}
