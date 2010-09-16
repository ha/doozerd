package paxos

type Packet struct {
	Msg
	Addr string
}

func (pk Packet) Id() string {
	m := pk.Msg.Dup().ClearFlags(Ack)
	return pk.Addr + " " + string(m.WireBytes())
}

type Putter interface {
	Put(m Msg)
}

type putCloser interface {
	Putter
	Close()
}

type ChanPutCloser chan Msg

func (cp ChanPutCloser) Put(m Msg) {
	go func() { cp <- m }()
}

func (cp ChanPutCloser) Close() {
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
