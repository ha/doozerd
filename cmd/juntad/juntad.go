package main

import (
	"flag"
	"fmt"
	"junta/client"
	"junta/lock"
	"junta/member"
	"junta/mon"
	"junta/paxos"
	"junta/server"
	"junta/session"
	"junta/store"
	"junta/util"
	"junta/web"
	"net"
	"os"
	"time"
)

const (
	alpha    = 50
	interval = 1e9 // ns == 1s
)

// Flags
var (
	listenAddr  = flag.String("l", "127.0.0.1:8046", "The address to bind to.")
	attachAddr  = flag.String("a", "", "The address of another node to attach to.")
	webAddr     = flag.String("w", ":8080", "Serve web requests on this address.")
	clusterName = flag.String("c", "local", "The non-empty cluster name.")
)

func activate(st *store.Store, self, prefix string, c *client.Client, cal chan int) {
	logger := util.NewLogger("activate")
	ch := make(chan store.Event)
	st.GetDirAndWatch("/junta/slot", ch)
	for ev := range ch {
		// TODO ev.IsEmpty()
		if ev.IsSet() && ev.Body == "" {
			_, err := c.Set(prefix+ev.Path, self, ev.Cas)
			if err != nil {
				logger.Println(err)
				continue
			}
			cal <- 1
			cal <- 1
			close(ch)
		}
	}
}

func advanceUntil(cl *client.Client, done chan int) {
	for _, ok := <-done; !ok; _, ok = <-done {
		cl.Nop()
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

	prefix := "/j/" + *clusterName

	if *listenAddr == "" {
		logger.Println("require a listen address")
		flag.Usage()
		os.Exit(1)
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

	cal := make(chan int, 2)

	var cl *client.Client
	self := util.RandId()
	st := store.New()
	seqn := uint64(0)
	if *attachAddr == "" { // we are the only node in a new cluster
		seqn = addPublicAddr(st, seqn+1, self, *listenAddr)
		seqn = addHostname(st, seqn+1, self, os.Getenv("HOSTNAME"))
		seqn = addMember(st, seqn+1, self, *listenAddr)
		seqn = claimSlot(st, seqn+1, "1", self)
		seqn = claimLeader(st, seqn+1, self)
		seqn = claimSlot(st, seqn+1, "2", "")
		seqn = claimSlot(st, seqn+1, "3", "")
		seqn = claimSlot(st, seqn+1, "4", "")
		seqn = claimSlot(st, seqn+1, "5", "")
		seqn = addPing(st, seqn+1, "pong")

		cal <- 1
		cal <- 1

		cl, err = client.Dial(*listenAddr)
		if err != nil {
			panic(err)
		}
	} else {
		cl, err = client.Dial(*attachAddr)
		if err != nil {
			panic(err)
		}

		path := prefix + "/junta/info/" + self + "/public-addr"
		_, err = cl.Set(path, *listenAddr, store.Clobber)
		if err != nil {
			panic(err)
		}

		path = prefix + "/junta/info/" + self + "/hostname"
		_, err = cl.Set(path, os.Getenv("HOSTNAME"), store.Clobber)
		if err != nil {
			panic(err)
		}

		var snap string
		seqn, snap, err = cl.Join(self, *listenAddr)
		if err != nil {
			panic(err)
		}

		done := make(chan int)
		st.Ops <- store.Op{1, snap}

		go advanceUntil(cl, done)

		go func() {
			st.Sync(seqn + alpha)
			close(done)
			activate(st, self, prefix, cl, cal)
		}()

		// TODO sink needs a way to pick up missing values if there are any
		// gaps in its sequence
	}
	mg := paxos.NewManager(self, seqn, alpha, st, st.Ops, outs)

	if *attachAddr == "" {
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		for i := seqn + 1; i < seqn+alpha; i++ {
			st.Ops <- store.Op{i, store.Nop}
		}
	}

	go func() {
		<-cal
		lock.New(st, mg)
		session.New(st, mg)
		go member.Clean(st, mg)
	}()

	sv := &server.Server{*listenAddr, st, mg, self, prefix}

	go func() {
		cas := store.Missing
		for _ = range time.Tick(interval) {
			_, cas, err = cl.Checkin(self, cas)
			if err != nil {
				logger.Println(err)
			}
		}
	}()

	go func() {
		panic(mon.Monitor(self, prefix, st, cl))
	}()

	go func() {
		panic(sv.Serve(listener, cal))
	}()

	if webListener != nil {
		web.Store = st
		web.ClusterName = *clusterName
		go web.Serve(webListener)
	}

	panic(sv.ListenAndServeUdp(outs))
}

func addPublicAddr(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	path := "/junta/info/" + self + "/public-addr"
	mx, err := store.EncodeSet(path, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func addHostname(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	path := "/junta/info/" + self + "/hostname"
	mx, err := store.EncodeSet(path, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func addMember(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/junta/members/"+self, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func claimSlot(st *store.Store, seqn uint64, slot, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/junta/slot/"+slot, self, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func claimLeader(st *store.Store, seqn uint64, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/junta/leader", self, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func addPing(st *store.Store, seqn uint64, v string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/ping", v, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}
