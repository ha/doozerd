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
	self := util.RandString(160)

	st := store.New()
	rg := paxos.NewRegistrar(self, st, alpha)

	addMember(st, self, *listenAddr)

	mg := paxos.NewManager(2, rg, paxos.ChanPutCloser(outs))
	go func() {
		panic(server.ListenAndServe(*listenAddr, st, mg))
	}()

	go func() {
		panic(server.ListenAndServeUdp(*listenAddr, mg, outs))
	}()

	for {
		st.Apply(mg.Recv())
	}
}

func addMember(st *store.Store, self, addr string) {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/j/junta/members/"+self, addr)
	if err != nil {
		panic(err)
	}
	st.Apply(1, mx)
}
