package paxos

type Message interface {
    Seqn() uint64
    From() uint64
    Cmd() string
    Body() string // soon to be []byte
}
