package main

import (
	"flag"
	"fmt"
	"http"
	"net"
	"os"

	"junta/paxos"
	"junta/mon"
	"junta/store"
	"junta/util"
	"junta/client"
	"junta/server"
	"junta/web"
)

const (
	alpha = 50
)

// Flags
var (
	listenAddr *string = flag.String("l", "", "The address to bind to. Must correspond to a single public interface.")
	publishAddr *string = flag.String("p", "", "Address to publish in junta for client connections.")
	attachAddr *string = flag.String("a", "", "The address of another node to attach to.")
	webAddr *string = flag.String("w", "", "Serve web requests on this address.")
)

func activate(st *store.Store, self, prefix string, c *client.Client) {
	logger := util.NewLogger("activate")
	ch := make(chan store.Event)
	st.Watch("/junta/slot/*", ch)
	for ev := range ch {
		// TODO ev.IsEmpty()
		if ev.IsSet() && ev.Body == "" {
			_, err := c.Set(prefix+ev.Path, self, ev.Cas)
			if err == nil {
				return
			}
			logger.Log(err)
		}
	}
}

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] <cluster-name>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}

func main() {
	util.LogWriter = os.Stderr
	logger := util.NewLogger("main")

	flag.Parse()
	flag.Usage = Usage

	if len(flag.Args()) < 1 {
		logger.Log("require a cluster name")
		flag.Usage()
		os.Exit(1)
	}

	clusterName := flag.Arg(0)
	prefix := "/j/" + clusterName

	if *listenAddr == "" {
		logger.Log("require a listen address")
		flag.Usage()
		os.Exit(1)
	}

	if *publishAddr == "" {
		*publishAddr = *listenAddr
	}

	var webListener net.Listener
	if *webAddr != "" {
		wl, err := net.Listen("tcp", *webAddr)
		if err != nil {
			panic(err)
		}
		webListener = wl
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		panic(err)
	}

	outs := make(paxos.ChanPutCloserTo)

	var cl *client.Client
	self := util.RandId()
	st := store.New()
	seqn := uint64(0)
	if *attachAddr == "" { // we are the only node in a new cluster
		seqn = addPublicAddr(st, seqn + 1, self, *publishAddr)
		seqn = addHostname(st, seqn + 1, self, os.Getenv("HOSTNAME"))
		seqn = addMember(st, seqn + 1, self, *listenAddr)
		seqn = claimSlot(st, seqn + 1, "1", self)
		seqn = claimLeader(st, seqn + 1, self)
		seqn = claimSlot(st, seqn + 1, "2", "")
		seqn = claimSlot(st, seqn + 1, "3", "")
		seqn = claimSlot(st, seqn + 1, "4", "")
		seqn = claimSlot(st, seqn + 1, "5", "")
		seqn = addPing(st, seqn + 1, "pong")

		cl, err = client.Dial(*listenAddr)
		if err != nil {
			panic(err)
		}
	} else {
		cl, err = client.Dial(*attachAddr)
		if err != nil {
			panic(err)
		}

		path := prefix + "/junta/info/"+ self +"/public-addr"
		_, err = cl.Set(path, *publishAddr, store.Clobber)
		if err != nil {
			panic(err)
		}

		path = prefix + "/junta/info/"+ self +"/hostname"
		_, err = cl.Set(path, os.Getenv("HOSTNAME"), store.Clobber)
		if err != nil {
			panic(err)
		}

		var snap string
		seqn, snap, err = cl.Join(self, *listenAddr)
		if err != nil {
			panic(err)
		}

		ch := make(chan store.Event)
		st.Wait(seqn + alpha, ch)
		st.Apply(1, snap)

		go func() {
			<-ch
			activate(st, self, prefix, cl)
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

	sv := &server.Server{*listenAddr, st, mg, self, prefix}

	go func() {
		panic(mon.Monitor(self, prefix, st, cl))
	}()

	go func() {
		panic(sv.Serve(listener))
	}()

	go func() {
		panic(sv.ListenAndServeUdp(outs))
	}()

	if webListener != nil {
		web.Store = st
		web.MainInfo.ClusterName = clusterName
		// http handlers are installed in the init function of junta/web.
		go http.Serve(webListener, nil)
	}

	for {
		st.Apply(mg.Recv())
	}
}

func addPublicAddr(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	path := "/junta/info/"+ self +"/public-addr"
	mx, err := store.EncodeSet(path, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}

func addHostname(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	path := "/junta/info/"+ self +"/hostname"
	mx, err := store.EncodeSet(path, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}

func addMember(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/junta/members/"+self, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}

func claimSlot(st *store.Store, seqn uint64, slot, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/junta/slot/"+slot, self, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}

func claimLeader(st *store.Store, seqn uint64, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/junta/leader", self, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}

func addPing(st *store.Store, seqn uint64, v string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/ping", v, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Apply(seqn, mx)
	return seqn
}
