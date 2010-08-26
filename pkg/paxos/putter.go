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
	seqn uint64
	from uint8
	Putter
}

func (w PutWrapper) Put(m Message) {
	m.SetSeqn(w.seqn)
	m.SetFrom(w.from)
	w.Putter.Put(m)
}
