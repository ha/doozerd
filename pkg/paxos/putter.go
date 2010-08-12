package paxos

type Putter interface {
	Put(m Msg)
}
