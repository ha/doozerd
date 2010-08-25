package paxos

type Message interface {
    SeqnX() uint64
    FromX() int
    CmdX() int
    BodyX() string // soon to be []byte
}
