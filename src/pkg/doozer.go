package doozer

import (
	"doozer/client"
	"doozer/consensus"
	"doozer/gc"
	"doozer/lock"
	"doozer/member"
	"doozer/server"
	"doozer/session"
	"doozer/store"
	"doozer/web"
	"encoding/base32"
	"io"
	"net"
	"os"
	"time"
	"log"
)

const (
	alpha               = 50
	maxUDPLen           = 3000
	sessionPollInterval = 1e9 // ns == 1s
)

const calDir = "/ctl/cal"

var calGlob = store.MustCompileGlob(calDir + "/*")


type proposer struct {
	seqns chan int64
	props chan *consensus.Prop
	st    *store.Store
}


func (p *proposer) Propose(v []byte) (e store.Event) {
	for e.Mut != string(v) {
		n := <-p.seqns
		w, err := p.st.Wait(n)
		if err != nil {
			panic(err) // can't happen
		}
		p.props <- &consensus.Prop{n, v}
		e = <-w
	}
	return
}


func Main(clusterName, attachAddr string, udpConn net.PacketConn, listener, webListener net.Listener, pulseInterval, fillDelay, kickTimeout int64) {
	listenAddr := listener.Addr().String()

	var activateSeqn int64
	cal := make(chan bool, 1)
	useSelf := make(chan bool, 1)

	self := randId()
	st := store.New()
	if attachAddr == "" { // we are the only node in a new cluster
		set(st, "/ctl/node/"+self+"/addr", listenAddr, store.Missing)
		set(st, "/ctl/node/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Missing)
		set(st, "/ctl/node/"+self+"/version", Version, store.Missing)
		set(st, "/ctl/cal/0", self, store.Missing)

		cal <- true
		close(useSelf)
	} else {
		var cl *client.Client
		cl = client.New("local", attachAddr) // TODO use real cluster name

		setC(cl, "/ctl/node/"+self+"/addr", listenAddr, store.Clobber)
		setC(cl, "/ctl/node/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Clobber)
		setC(cl, "/ctl/node/"+self+"/version", Version, store.Clobber)

		rev, err := cl.Rev()
		if err != nil {
			panic(err)
		}

		walk, err := cl.Walk("/**", &rev, nil, nil)
		if err != nil {
			panic(err)
		}

		watch, err := cl.Watch("/**", rev+1)
		if err != nil {
			panic(err)
		}

		go follow(st.Ops, watch.C)
		follow(st.Ops, walk.C)
		st.Flush()
		ch, err := st.Wait(rev + 1)
		if err == nil {
			<-ch
		}

		go func() {
			activateSeqn = activate(st, self, cl)
			cal <- true
			advanceUntil(cl, st.Seqns, activateSeqn+alpha)
			err := watch.Cancel()
			if err != nil {
				panic(err)
			}
			close(useSelf)
		}()
	}

	pr := &proposer{
		seqns: make(chan int64, alpha),
		props: make(chan *consensus.Prop),
		st:    st,
	}

	start := <-st.Seqns
	cmw := st.Watch(store.Any)
	in := make(chan consensus.Packet, 50)
	out := make(chan consensus.Packet, 50)

	consensus.NewManager(self, start, alpha, in, out, st.Ops, pr.seqns, pr.props, cmw, fillDelay, st)

	if attachAddr == "" {
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		for i := start + 1; i < start+alpha+1; i++ {
			st.Ops <- store.Op{i, store.Nop}
		}
	}

	times := make(map[string]int64)
	shun := make(chan string, 3) // sufficient for a cluster of 7

	go member.Clean(shun, st, pr)

	go func() {
		<-cal
		go lock.Clean(pr, st.Watch(lock.SessGlob))
		go session.Clean(st, pr, time.Tick(sessionPollInterval))
		go gc.Pulse(self, st.Seqns, pr, pulseInterval)
		go gc.Clean(st, 360000, time.Tick(1e9))
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
			addr, err := net.ResolveUDPAddr(p.Addr)
			if err != nil {
				log.Println(err)
				continue
			}
			n, err := udpConn.WriteTo(p.Data, addr)
			if err != nil {
				log.Println(err)
				continue
			}
			if n != len(p.Data) {
				log.Println("packet len too long:", len(p.Data))
				continue
			}
		}
	}()

	var pt int64
	pi := kickTimeout / 2
	for {
		t := time.Nanoseconds()

		buf := make([]byte, maxUDPLen)
		n, addr, err := udpConn.ReadFrom(buf)
		if err == os.EINVAL {
			return
		}
		if err != nil {
			log.Println(err)
			continue
		}

		buf = buf[:n]

		// Update liveness time stamp for this addr
		times[addr.String()] = t

		if t > pt+pi {
			n := t - kickTimeout
			for addr, s := range times {
				if n > s && addr != self {
					times[addr] = 0, false
					shun <- addr
				}
			}
		}
		pt = t

		in <- consensus.Packet{addr.String(), buf}
	}
}

func activate(st *store.Store, self string, c *client.Client) int64 {
	w := store.NewWatch(st, calGlob)

	for _, base := range store.Getdir(st, calDir) {
		p := calDir + "/" + base
		v, rev := st.Get(p)
		if rev != store.Dir && v[0] == "" {
			seqn, err := c.Set(p, rev, []byte(self))
			if err != nil {
				log.Println(err)
				continue
			}

			w.Stop()
			return seqn
		}
	}

	for ev := range w.C {
		// TODO ev.IsEmpty()
		if ev.IsSet() && ev.Body == "" {
			seqn, err := c.Set(ev.Path, ev.Rev, []byte(self))
			if err != nil {
				log.Println(err)
				continue
			}
			w.Stop()
			return seqn
		}
	}

	return 0
}

func advanceUntil(cl *client.Client, ver <-chan int64, done int64) {
	for <-ver < done {
		cl.Nop()
	}
}

func set(st *store.Store, path, body string, rev int64) {
	mut := store.MustEncodeSet(path, body, rev)
	st.Ops <- store.Op{1 + <-st.Seqns, mut}
}

func setC(cl *client.Client, path, body string, rev int64) {
	_, err := cl.Set(path, rev, []byte(body))
	if err != nil {
		panic(err)
	}
}

func follow(ops chan<- store.Op, ch <-chan *client.Event) {
	for ev := range ch {
		// store.Clobber is okay here because the event
		// has already passed through another store
		mut := store.MustEncodeSet(ev.Path, string(ev.Body), store.Clobber)
		ops <- store.Op{ev.Rev, mut}
	}
}


func randId() string {
	const bits = 80 // enough for 10**8 ids with p(collision) < 10**-8
	rnd := make([]byte, bits/8)

	f, err := os.Open("/dev/urandom", os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}

	n, err := io.ReadFull(f, rnd)
	if err != nil {
		panic(err)
	}
	if n != len(rnd) {
		panic("io.ReadFull len mismatch")
	}

	enc := make([]byte, base32.StdEncoding.EncodedLen(len(rnd)))
	base32.StdEncoding.Encode(enc, rnd)
	return string(enc)
}
