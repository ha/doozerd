package main

import (
	"flag"
	"os"

	"junta/paxos"
	"junta/store"
	"junta/util"
	"junta/server"
)

const (
	alpha = 50
	idBits = 160
)


// Flags
var (
	listenAddr *string = flag.String("l", ":8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

func main() {
	flag.Parse()

	util.LogWriter = os.Stderr

	outs := make(chan paxos.Msg)

	self := util.RandHexString(idBits)
	st := store.New()

	seqn := uint64(1)
	seqn = addMember(st, seqn, self, *listenAddr)

	mg := paxos.NewManager(self, seqn, alpha, st, paxos.ChanPutCloser(outs))

	sv := &server.Server{*listenAddr, st, mg}

	go func() {
		panic(sv.ListenAndServe())
	}()

	go func() {
		panic(sv.ListenAndServeUdp(outs))
	}()

	for {
		st.Apply(mg.Recv())
	}
}

func addMember(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/j/junta/members/"+self, addr, store.Clobber)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn + 1
}
