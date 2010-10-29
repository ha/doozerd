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

type PutterTo interface {
	PutTo(m Msg, addr string)
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

type ChanPutCloserTo chan Packet

func (cp ChanPutCloserTo) PutTo(m Msg, addr string) {
	go func() { cp <- Packet{m, addr} }()
}

func (cp ChanPutCloserTo) Close() {
	close(cp)
}

type putToWrapper struct {
	seqn uint64
	pt   PutterTo
}

func (w putToWrapper) PutTo(m Msg, addr string) {
	m.SetSeqn(w.seqn)
	w.pt.PutTo(m, addr)
}
