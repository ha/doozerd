package main

import (
	"flag"
	"fmt"
	"junta"
	"junta/paxos"
	"junta/store"
	"junta/util"
)

const alpha = 50

// Flags
var (
	listenAddr *string = flag.String("l", ":8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

func main() {
	flag.Parse()
	store.Logger = util.NewLogger("store")
	outs := make(chan paxos.Msg)
	self := randString(160)

	st := store.New()
	rg := paxos.NewRegistrar(self, st, alpha)

	addMember(st, self, *listenAddr)

	mg := paxos.NewManager(2, rg, paxos.ChanPutCloser(outs))
	go func() {
		panic(junta.ListenAndServe(*listenAddr, st, mg))
	}()

	go func() {
		panic(junta.ListenAndServeUdp(*listenAddr, mg, outs))
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

func randString(bits int) string {
	selfBytes := make([]byte, bits/8)
	util.RandBytes(selfBytes)
	return fmt.Sprintf("%x", selfBytes)
}
