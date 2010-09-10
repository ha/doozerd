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
	listenAddr *string = flag.String("l", "[::1]:8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

func main() {
	flag.Parse()

	util.LogWriter = os.Stderr

	outs := make(chan paxos.Msg)

	self := util.RandHexString(idBits)
	st := store.New()

	seqn := uint64(0)
	seqn = addMember(st, seqn + 1, self, *listenAddr)
	seqn = claimSlot(st, seqn + 1, "1", self)
	seqn = claimLeader(st, seqn + 1, self)

	mg := paxos.NewManager(self, seqn, alpha, st, paxos.ChanPutCloser(outs))

	// Skip ahead alpha steps so that the registrar can provide a meaningful
	// cluster.
	for i := seqn + 1; i < seqn + alpha; i++ {
		go st.Apply(i, "") // nop
	}

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
	mx, err := store.EncodeSet("/j/junta/members/"+self, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}

func claimSlot(st *store.Store, seqn uint64, slot, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/j/junta/slot/"+slot, self, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}

func claimLeader(st *store.Store, seqn uint64, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/j/junta/leader", self, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}
