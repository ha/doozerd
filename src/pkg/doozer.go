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

func Main(clusterName, attachAddr string, udpConn net.PacketConn, listener, webListener net.Listener) {
	logger := util.NewLogger("main")

	var err os.Error

	listenAddr := listener.Addr().String()

	outs := make(paxos.ChanPutCloserTo)

	cal := make(chan int)

	var cl *client.Client
	self := util.RandId()
	st := store.New()
	if attachAddr == "" { // we are the only node in a new cluster
		set(st, "/doozer/info/"+self+"/public-addr", listenAddr, store.Missing)
		set(st, "/doozer/info/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Missing)
		set(st, "/doozer/members/"+self, listenAddr, store.Missing)
		set(st, "/doozer/slot/"+"1", self, store.Missing)
		set(st, "/doozer/leader", self, store.Missing)
		set(st, "/ping", "pong", store.Missing)

		close(cal)

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

		joinSeqn, snap, err := cl.Join(self, listenAddr)
		if err != nil {
			panic(err)
		}

		done := make(chan int)
		st.Ops <- store.Op{1, snap}

		go advanceUntil(cl, done)

		go func() {
			st.Sync(joinSeqn + alpha)
			close(done)
			activate(st, self, cl, cal)
		}()

		// TODO sink needs a way to pick up missing values if there are any
		// gaps in its sequence
	}

	mg := paxos.NewManager(self, alpha, st, outs)

	if attachAddr == "" {
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		n := <-st.Seqns
		for i := n + 1; i < n+alpha; i++ {
			st.Ops <- store.Op{i, store.Nop}
		}
	}

	go func() {
		<-cal
		go lock.Clean(st, mg)
		go session.Clean(st, mg)
		go member.Clean(st, mg)
		go gc.Pulse(self, st.Seqns, cl, pulseInterval)
		go gc.Clean(st)
	}()

	sv := &server.Server{udpConn, listenAddr, st, mg, self}

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
		err := sv.Serve(listener, cal)
		if err != nil {
			panic(err)
		}
	}()

	if webListener != nil {
		web.Store = st
		web.ClusterName = clusterName
		go web.Serve(webListener)
	}

	sv.ServeUdp(outs)
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
			close(cal)
			close(ch)
		}
	}
}

func advanceUntil(cl *client.Client, done chan int) {
	for _, ok := <-done; !ok; _, ok = <-done {
		cl.Noop()
	}
}

func set(st *store.Store, path, body, cas string) {
	mut := store.MustEncodeSet(path, body, cas)
	st.Ops <- store.Op{1 + <-st.Seqns, mut}
}
