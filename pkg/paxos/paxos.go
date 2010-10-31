package paxos

import (
	"net"
	"os"
)

type ReadFromer interface {
	ReadFrom(b []byte) (n int, addr net.Addr, err os.Error)
}

type Proposer interface {
	Propose(v string) (seqn uint64, err os.Error)
}
