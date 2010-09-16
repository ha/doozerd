package main

import (
	"flag"
	"net"
	"os"

	"junta/paxos"
	"junta/proc"
	"junta/store"
	"junta/util"
	"junta/client"
	"junta/server"
)

const (
	alpha = 50
	idBits = 160
)


// Flags
var (
	listenAddr *string = flag.String("l", "", "The address to bind to. Must correspond to a single public interface.")
	attachAddr *string = flag.String("a", "", "The address of another node to attach to.")
)

func activate(st *store.Store, self string, c client.Conn) {
	logger := util.NewLogger("activate")
	ch := make(chan store.Event)
	st.Watch("/j/junta/slot/*", ch)
	for ev := range ch {
		// TODO ev.IsEmpty()
		if ev.IsSet() && ev.Body == "" {
			_, err := client.Set(c, ev.Path, self, ev.Cas)
			if err == nil {
				return
			}
			logger.Log(err)
		}
	}
}

func main() {
	flag.Parse()

	util.LogWriter = os.Stderr
	logger := util.NewLogger("main")

	if *listenAddr == "" {
		logger.Log("require a listen address")
		flag.Usage()
		os.Exit(1)
	}

	outs := make(paxos.ChanPutCloserTo)

	self := util.RandHexString(idBits)
	st := store.New()
	seqn := uint64(0)
	if *attachAddr == "" { // we are the only node in a new cluster
		seqn = addMember(st, seqn + 1, self, *listenAddr)
		seqn = claimSlot(st, seqn + 1, "1", self)
		seqn = claimLeader(st, seqn + 1, self)
	} else {
		c, err := client.Dial(*attachAddr)
		if err != nil {
			panic(err)
		}

		var snap string
		seqn, snap, err = client.Join(c, self, *listenAddr)
		if err != nil {
			panic(err)
		}

		ch := make(chan store.Event)
		st.Wait(seqn + alpha, ch)
		st.Apply(1, snap)

		go func() {
			<-ch
			activate(st, self, c)
		}()

		// TODO sink needs a way to pick up missing values if there are any
		// gaps in its sequence
	}
	mg := paxos.NewManager(self, seqn, alpha, st, outs)

	if *attachAddr == "" {
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		for i := seqn + 1; i < seqn + alpha; i++ {
			go st.Apply(i, store.Nop)
		}
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		panic(err)
	}

	sv := &server.Server{*listenAddr, st, mg}

	go func() {
		panic(proc.Monitor(self, st))
	}()

	go func() {
		panic(sv.Serve(listener))
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
