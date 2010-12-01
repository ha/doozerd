package doozer

import (
	"doozer/client"
	"doozer/gc"
	"doozer/lock"
	"doozer/member"
	"doozer/paxos"
	"doozer/server"
	"doozer/session"
	"doozer/store"
	"doozer/util"
	"doozer/web"
	"net"
	"os"
	"time"
)

const (
	alpha           = 50
	checkinInterval = 1e9 // ns == 1s
	pulseInterval   = 1e9
)

func Main(clusterName, attachAddr, webAddr string, listener net.Listener) {
	logger := util.NewLogger("main")

	var webListener net.Listener
	if webAddr != "" {
		wl, err := net.Listen("tcp", webAddr)
		if err != nil {
			panic(err)
		}
		webListener = wl
	}

	var err os.Error

	listenAddr := listener.Addr().String()

	outs := make(paxos.ChanPutCloserTo)

	cal := make(chan int, 2)

	var cl *client.Client
	self := util.RandId()
	st := store.New()
	seqn := uint64(0)
	if attachAddr == "" { // we are the only node in a new cluster
		seqn = addPublicAddr(st, seqn+1, self, listenAddr)
		seqn = addHostname(st, seqn+1, self, os.Getenv("HOSTNAME"))
		seqn = addMember(st, seqn+1, self, listenAddr)
		seqn = claimSlot(st, seqn+1, "1", self)
		seqn = claimLeader(st, seqn+1, self)
		seqn = addPing(st, seqn+1, "pong")

		cal <- 1
		cal <- 1

		cl, err = client.Dial(listenAddr)
		if err != nil {
			panic(err)
		}
	} else {
		cl, err = client.Dial(attachAddr)
		if err != nil {
			panic(err)
		}

		path := "/doozer/info/" + self + "/public-addr"
		_, err = cl.Set(path, listenAddr, store.Clobber)
		if err != nil {
			panic(err)
		}

		path = "/doozer/info/" + self + "/hostname"
		_, err = cl.Set(path, os.Getenv("HOSTNAME"), store.Clobber)
		if err != nil {
			panic(err)
		}

		var snap string
		seqn, snap, err = cl.Join(self, listenAddr)
		if err != nil {
			panic(err)
		}

		done := make(chan int)
		st.Ops <- store.Op{1, snap}

		go advanceUntil(cl, done)

		go func() {
			st.Sync(seqn + alpha)
			close(done)
			activate(st, self, cl, cal)
		}()

		// TODO sink needs a way to pick up missing values if there are any
		// gaps in its sequence
	}

	mg := paxos.NewManager(self, seqn, alpha, st, st.Ops, outs)

	if attachAddr == "" {
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		for i := seqn + 1; i < seqn+alpha; i++ {
			st.Ops <- store.Op{i, store.Nop}
		}
	}

	go func() {
		<-cal
		go lock.Clean(st, mg)
		go session.Clean(st, mg)
		go member.Clean(st, mg)
		go gc.Pulse(self, st.Seqns, cl, pulseInterval)
	}()

	sv := &server.Server{listenAddr, st, mg, self}

	go func() {
		cas := store.Missing
		for _ = range time.Tick(checkinInterval) {
			_, cas, err = cl.Checkin(self, cas)
			if err != nil {
				logger.Println(err)
			}
		}
	}()

	go func() {
		panic(sv.Serve(listener, cal))
	}()

	if webListener != nil {
		web.Store = st
		web.ClusterName = clusterName
		go web.Serve(webListener)
	}

	panic(sv.ListenAndServeUdp(outs))
}

func activate(st *store.Store, self string, c *client.Client, cal chan int) {
	logger := util.NewLogger("activate")
	ch := make(chan store.Event)
	st.GetDirAndWatch("/doozer/slot", ch)
	for ev := range ch {
		// TODO ev.IsEmpty()
		if ev.IsSet() && ev.Body == "" {
			_, err := c.Set(ev.Path, self, ev.Cas)
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
		cl.Noop()
	}
}


func addPublicAddr(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	path := "/doozer/info/" + self + "/public-addr"
	mx, err := store.EncodeSet(path, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func addHostname(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	path := "/doozer/info/" + self + "/hostname"
	mx, err := store.EncodeSet(path, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func addMember(st *store.Store, seqn uint64, self, addr string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/doozer/members/"+self, addr, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func claimSlot(st *store.Store, seqn uint64, slot, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/doozer/slot/"+slot, self, store.Missing)
	if err != nil {
		panic(err)
	}
	st.Ops <- store.Op{seqn, mx}
	return seqn
}

func claimLeader(st *store.Store, seqn uint64, self string) uint64 {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/doozer/leader", self, store.Missing)
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
