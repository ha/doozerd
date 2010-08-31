package paxos

import (
	"junta/util"
	"net"
	"os"
)

type ReadFromer interface {
	ReadFrom(b []byte) (n int, addr net.Addr, err os.Error)
}

var logger = util.NullLogger
