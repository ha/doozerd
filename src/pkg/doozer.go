package doozer

import (
	"doozer/ack"
	"doozer/client"
	"doozer/gc"
	"doozer/lock"
	"doozer/member"
	"doozer/consensus"
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
	timeout         = 60e9 // 60s
)

const slot = "/doozer/slot"

var slots = store.MustCompileGlob("/doozer/slot/*")


type proposer struct {
	seqns chan int64
	props chan *consensus.Prop
	st    *store.Store
}


func (p *proposer) Propose(v []byte) (e store.Event) {
	for e.Mut != string(v) {
		n := <-p.seqns
		w := p.st.Wait(n)
		p.props <- &consensus.Prop{n, v}
		e = <-w
	}
	return
}


func Main(clusterName, attachAddr string, udpConn net.PacketConn, listener, webListener net.Listener) {
	logger := util.NewLogger("main")

	var err os.Error

	listenAddr := listener.Addr().String()

	var activateSeqn int64
	cal := make(chan bool, 1)
	useSelf := make(chan bool, 1)

	var cl *client.Client
	self := util.RandId()
	st := store.New()
	if attachAddr == "" { // we are the only node in a new cluster
		set(st, "/doozer/info/"+self+"/addr", listenAddr, store.Missing)
		set(st, "/doozer/info/"+self+"/public-addr", listenAddr, store.Missing)
		set(st, "/doozer/info/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Missing)
		set(st, "/doozer/info/"+self+"/version", Version, store.Missing)
		set(st, "/doozer/members/"+self, listenAddr, store.Missing)
		set(st, "/doozer/slot/"+"1", self, store.Missing)
		set(st, "/ping", "pong", store.Missing)

		cal <- true
		useSelf <- true

		cl = client.New("local", listenAddr) // TODO use real cluster name
	} else {
		cl = client.New("local", attachAddr) // TODO use real cluster name

		path := "/doozer/info/" + self + "/addr"
		_, err = cl.Set(path, store.Clobber, []byte(listenAddr))
		if err != nil {
			panic(err)
		}

		path = "/doozer/info/" + self + "/public-addr"
		_, err = cl.Set(path, store.Clobber, []byte(listenAddr))
		if err != nil {
			panic(err)
		}

		path = "/doozer/info/" + self + "/hostname"
		_, err = cl.Set(path, store.Clobber, []byte(os.Getenv("HOSTNAME")))
		if err != nil {
			panic(err)
		}

		path = "/doozer/info/" + self + "/version"
		_, err = cl.Set(path, store.Clobber, []byte(Version))
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

			activateSeqn = activate(st, self, cl)
			cal <- true

			done = make(chan int)
			go advanceUntil(cl, done)
			st.Sync(activateSeqn + alpha)
			close(done)
			useSelf <- true
		}()

		// TODO sink needs a way to pick up missing values if there are any
		// gaps in its sequence
	}

	acker := ack.Ackify(udpConn)

	pr := &proposer{
		seqns: make(chan int64, alpha),
		props: make(chan *consensus.Prop),
		st:    st,
	}

	start := <-st.Seqns
	<-st.Wait(start)
	cmw := st.Watch(store.Any)
	in := make(chan consensus.Packet)
	out := make(chan consensus.Packet)

	consensus.NewManager(self, alpha, in, out, st.Ops, pr.seqns, pr.props, cmw)

	if attachAddr == "" {
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		for i := start+1; i < start+alpha+1; i++ {
			st.Ops <- store.Op{i, store.Nop}
		}
	}

	live := make(chan string, 64)
	shun := make(chan string, 3) // sufficient for a cluster of 7

	go member.Clean(shun, st, pr)
	go member.Timeout(live, shun, listenAddr, timeout)

	go func() {
		<-cal
		go lock.Clean(st, pr)
		go session.Clean(st, pr)
		go gc.Pulse(self, st.Seqns, pr, pulseInterval)
		go gc.Clean(st)
	}()

	sv := &server.Server{listenAddr, st, pr, self, alpha}

	go sv.Serve(listener, useSelf)

	if webListener != nil {
		web.Store = st
		web.ClusterName = clusterName
		go web.Serve(webListener)
	}

	go func() {
		for p := range out {
			acker.WriteTo(p.Data, p.Addr)
		}
	}()

	for {
		data, addr, err := acker.ReadFrom()
		if err == os.EINVAL {
			break
		}
		if err != nil {
			logger.Println(err)
			continue
		}

		// Update liveness time stamp for this addr
		live <- addr

		in <- consensus.Packet{addr, data}
	}
}

func activate(st *store.Store, self string, c *client.Client) (seqn int64) {
	logger := util.NewLogger("activate")
	w := store.NewWatch(st, slots)
	var err os.Error

	for _, base := range store.Getdir(st, slot) {
		p := slot + "/" + base
		v, cas := st.Get(p)
		if cas != store.Dir && v[0] == "" {
			seqn, err = c.Set(p, cas, []byte(self))
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
			seqn, err = c.Set(ev.Path, ev.Cas, []byte(self))
			if err != nil {
				logger.Println(err)
				continue
			}
			w.Stop()
			close(w.C)
		}
	}

	return seqn
}

func advanceUntil(cl *client.Client, done chan int) {
	for {
		select {
		case <-done:
			return
		default:
		}

		cl.Noop()
	}
}

func set(st *store.Store, path, body string, cas int64) {
	mut := store.MustEncodeSet(path, body, cas)
	st.Ops <- store.Op{1 + <-st.Seqns, mut}
}
