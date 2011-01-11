package paxos

import (
	"doozer/store"
	"net"
	"os"
)

type ReadFromer interface {
	ReadFrom(b []byte) (n int, addr net.Addr, err os.Error)
}

type Proposer interface {
	Propose(v string, cancel chan bool) (seqn uint64, cas string, err os.Error)
}

func Set(p Proposer, path, body, cas string, cancel chan bool) (uint64, string, os.Error) {
	mut, err := store.EncodeSet(path, body, cas)
	if err != nil {
		return 0, "", err
	}

	return p.Propose(mut, cancel)
}

func Del(p Proposer, path, cas string, cancel chan bool) os.Error {
	mut, err := store.EncodeDel(path, cas)
	if err != nil {
		return err
	}

	_, _, err = p.Propose(mut, cancel)
	return err
}
