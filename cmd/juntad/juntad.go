package main

import (
	"flag"
	"os"

	"junta/paxos"
	"junta/store"
	"junta/util"
	"junta/server"
)

const alpha = 50

// Flags
var (
	listenAddr *string = flag.String("l", ":8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

func main() {
	flag.Parse()

	util.LogWriter = os.Stderr

	outs := make(chan paxos.Msg)

	st := store.New()
	mg := paxos.NewManager(2, alpha, st, paxos.ChanPutCloser(outs))

	addMember(st, mg.Self, *listenAddr)

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

func addMember(st *store.Store, self, addr string) {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/j/junta/members/"+self, addr, store.Clobber)
	if err != nil {
		panic(err)
	}
	st.Apply(1, mx)
}
