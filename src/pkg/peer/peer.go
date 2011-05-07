package peer

import (
	"doozer/consensus"
	"doozer/gc"
	"doozer/member"
	"doozer/server"
	"doozer/store"
	"doozer/web"
	"github.com/ha/doozer"
	"net"
	"os"
	"time"
	"log"
)

const (
	alpha     = 50
	maxUDPLen = 3000
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
		w, err := p.st.Wait(store.Any, n)
		if err != nil {
			panic(err) // can't happen
		}
		p.props <- &consensus.Prop{n, v}
		e = <-w
	}
	return
}


func Main(clusterName, self, buri, rwsk, rosk string, cl *doozer.Conn, udpConn net.PacketConn, listener, webListener net.Listener, pulseInterval, fillDelay, kickTimeout int64) {
	listenAddr := listener.Addr().String()

	canWrite := make(chan bool, 1)
	in := make(chan consensus.Packet, 50)
	out := make(chan consensus.Packet, 50)

	st := store.New()
	pr := &proposer{
		seqns: make(chan int64, alpha),
		props: make(chan *consensus.Prop),
		st:    st,
	}

	calSrv := func(start int64) {
		go gc.Pulse(self, st.Seqns, pr, pulseInterval)
		go gc.Clean(st, 360000, time.Tick(1e9))
		consensus.NewManager(self, start, alpha, in, out, st.Ops, pr.seqns, pr.props, fillDelay, st)
	}

	if cl == nil { // we are the only node in a new cluster
		set(st, "/ctl/name", clusterName, store.Missing)
		set(st, "/ctl/node/"+self+"/addr", listenAddr, store.Missing)
		set(st, "/ctl/node/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Missing)
		set(st, "/ctl/node/"+self+"/version", Version, store.Missing)
		set(st, "/ctl/cal/0", self, store.Missing)
		calSrv(<-st.Seqns)
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		for i := 0; i < alpha; i++ {
			st.Ops <- store.Op{1 + <-st.Seqns, store.Nop}
		}
		canWrite <- true
	} else {
		setC(cl, "/ctl/node/"+self+"/addr", listenAddr, store.Clobber)
		setC(cl, "/ctl/node/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Clobber)
		setC(cl, "/ctl/node/"+self+"/version", Version, store.Clobber)

		rev, err := cl.Rev()
		if err != nil {
			panic(err)
		}

		stop := make(chan bool, 1)
		go follow(st, cl, rev+1, stop)

		errs := make(chan os.Error)
		go func() {
			e, ok := <-errs
			if ok {
				panic(e)
			}
		}()
		doozer.Walk(cl, rev, "/", cloner{st.Ops, cl}, errs)
		close(errs)
		st.Flush()

		ch, err := st.Wait(store.Any, rev+1)
		if err == nil {
			<-ch
		}

		go func() {
			n := activate(st, self, cl)
			calSrv(n)
			advanceUntil(cl, st.Seqns, n+alpha)
			stop <- true
			canWrite <- true
			if buri != "" {
				b, err := doozer.DialUri(buri)
				if err != nil {
					panic(err)
				}
				setC(
					b,
					"/ctl/ns/"+clusterName+"/"+self,
					listenAddr,
					store.Missing,
				)
			}
		}()
	}

	shun := make(chan string, 3) // sufficient for a cluster of 7
	go member.Clean(shun, st, pr)
	go server.ListenAndServe(listener, canWrite, st, pr, rwsk, rosk)

	if rwsk == "" && rosk == "" && webListener != nil {
		web.Store = st
		web.ClusterName = clusterName
		go web.Serve(webListener)
	}

	go func() {
		for p := range out {
			addr, err := net.ResolveUDPAddr("udp", p.Addr)
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

	lv := liveness{
		timeout: kickTimeout,
		ival:    kickTimeout / 2,
		times:   make(map[string]int64),
		self:    self,
		shun:    shun,
	}
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
		lv.times[addr.String()] = t
		lv.check(t)

		in <- consensus.Packet{addr.String(), buf}
	}
}


func activate(st *store.Store, self string, c *doozer.Conn) int64 {
	rev, _ := st.Snap()

	for _, base := range store.Getdir(st, calDir) {
		p := calDir + "/" + base
		v, rev := st.Get(p)
		if rev != store.Dir && v[0] == "" {
			seqn, err := c.Set(p, rev, []byte(self))
			if err != nil {
				log.Println(err)
				continue
			}

			return seqn
		}
	}

	for {
		ch, err := st.Wait(calGlob, rev+1)
		if err != nil {
			panic(err)
		}
		ev, ok := <-ch
		if !ok {
			panic(os.EOF)
		}
		rev = ev.Rev
		// TODO ev.IsEmpty()
		if ev.IsSet() && ev.Body == "" {
			seqn, err := c.Set(ev.Path, ev.Rev, []byte(self))
			if err != nil {
				log.Println(err)
				continue
			}
			return seqn
		}
	}

	return 0
}

func advanceUntil(cl *doozer.Conn, ver <-chan int64, done int64) {
	for <-ver < done {
		cl.Nop()
	}
}

func set(st *store.Store, path, body string, rev int64) {
	mut := store.MustEncodeSet(path, body, rev)
	st.Ops <- store.Op{1 + <-st.Seqns, mut}
}

func setC(cl *doozer.Conn, path, body string, rev int64) {
	_, err := cl.Set(path, rev, []byte(body))
	if err != nil {
		panic(err)
	}
}

func follow(st *store.Store, cl *doozer.Conn, rev int64, stop chan bool) {
	for {
		ev, err := cl.Wait("/**", rev)
		if err != nil {
			panic(err)
		}

		// store.Clobber is okay here because the event
		// has already passed through another store
		mut := store.MustEncodeSet(ev.Path, string(ev.Body), store.Clobber)
		st.Ops <- store.Op{ev.Rev, mut}
		rev = ev.Rev + 1

		select {
		case <-stop:
			return
		default:
		}
	}
}

type cloner struct {
	ch chan<- store.Op
	cl *doozer.Conn
}

func (c cloner) VisitDir(path string, f *doozer.FileInfo) bool {
	return true
}

func (c cloner) VisitFile(path string, f *doozer.FileInfo) {
	// store.Clobber is okay here because the event
	// has already passed through another store
	body, _, err := c.cl.Get(path, &f.Rev)
	if err != nil {
		panic(err)
	}
	mut := store.MustEncodeSet(path, string(body), store.Clobber)
	c.ch <- store.Op{f.Rev, mut}
}
