package doozer

import (
	"doozer/ack"
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
)

const (
	alpha           = 50
	pulseInterval   = 1e9
)

const slot = "/doozer/slot"

var slots = store.MustCompileGlob("/doozer/slot/*")


func Main(clusterName, attachAddr string, udpConn net.PacketConn, listener, webListener net.Listener) {
	logger := util.NewLogger("main")

	var err os.Error

	listenAddr := listener.Addr().String()

	cal := make(chan int)

	var cl *client.Client
	self := util.RandId()
	st := store.New()
	if attachAddr == "" { // we are the only node in a new cluster
		set(st, "/doozer/info/"+self+"/public-addr", listenAddr, store.Missing)
		set(st, "/doozer/info/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Missing)
		set(st, "/doozer/members/"+self, listenAddr, store.Missing)
		set(st, "/doozer/slot/"+"1", self, store.Missing)
		set(st, "/ping", "pong", store.Missing)

		close(cal)

		cl = client.New("local", listenAddr) // TODO use real cluster name
	} else {
		cl = client.New("local", attachAddr) // TODO use real cluster name

		path := "/doozer/info/" + self + "/public-addr"
		_, err = cl.Set(path, store.Clobber, []byte(listenAddr))
		if err != nil {
			panic(err)
		}

		path = "/doozer/info/" + self + "/hostname"
		_, err = cl.Set(path, store.Clobber, []byte(os.Getenv("HOSTNAME")))
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
			activate(st, self, cl)
			close(cal)
		}()

		// TODO sink needs a way to pick up missing values if there are any
		// gaps in its sequence
	}

	acker := ack.Ackify(udpConn)

	mg := paxos.NewManager(self, alpha, st, paxos.Encoder{acker})

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

	sv := &server.Server{listenAddr, st, mg, self}

	go func() {
		cas := store.Missing
		for {
			cas, err = cl.Checkin(self, cas)
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

	decoder := paxos.Decoder{mg}
	for {
		data, addr, err := acker.ReadFrom()
		if err == os.EINVAL {
			break
		}
		if err != nil {
			logger.Println(err)
			continue
		}
		decoder.WriteFrom(addr, data)
	}
}

func activate(st *store.Store, self string, c *client.Client) {
	logger := util.NewLogger("activate")
	w := store.NewWatch(st, slots)

	for _, base := range store.GetDir(st, slot) {
		p := slot + "/" + base
		v, cas := st.Get(p)
		if cas != store.Dir && v[0] == "" {
			_, err := c.Set(p, cas, []byte(self))
			if err != nil {
				logger.Println(err)
				continue
			}

			w.Stop()
			close(w.C)
			break
		}
	}

	for ev := range w.C {
		// TODO ev.IsEmpty()
		if ev.IsSet() && ev.Body == "" {
			_, err := c.Set(ev.Path, ev.Cas, []byte(self))
			if err != nil {
				logger.Println(err)
				continue
			}
			w.Stop()
			close(w.C)
		}
	}
}

func advanceUntil(cl *client.Client, done chan int) {
	for _, ok := <-done; !ok; _, ok = <-done {
		cl.Noop()
	}
}

func set(st *store.Store, path, body string, cas int64) {
	mut := store.MustEncodeSet(path, body, cas)
	st.Ops <- store.Op{1 + <-st.Seqns, mut}
}
