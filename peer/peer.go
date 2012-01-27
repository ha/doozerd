package peer

import (
	"github.com/ha/doozerd/consensus"
	"github.com/ha/doozerd/gc"
	"github.com/ha/doozerd/member"
	"github.com/ha/doozerd/server"
	"github.com/ha/doozerd/store"
	"github.com/ha/doozerd/web"
	"github.com/ha/doozer"
	"io"
	"log"
	"net"
	"os"
	"time"
)

const (
	alpha     = 50
	maxUDPLen = 3000
)

const calDir = "/ctl/cal"

var calGlob = store.MustCompileGlob(calDir + "/*")
var Version = "unknown" // set in version.go, uses makefiles.

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

// BUG: too many parameters.
func Main(clusterName, self, buri, rwsk, rosk, journal string, cl *doozer.Conn, udpConn *net.UDPConn, listener, webListener net.Listener, pulseInterval, fillDelay, kickTimeout int64, hi int64) {
	listenAddr := listener.Addr().String()

	canWrite := make(chan bool, 1)
	in := make(chan consensus.Packet, 50)
	out := make(chan consensus.Packet, 50)

	st := store.New(journal)
	pr := &proposer{
		seqns: make(chan int64, alpha),
		props: make(chan *consensus.Prop),
		st:    st,
	}

	calSrv := func(start int64) {
		go gc.Pulse(self, st.Seqns, pr, pulseInterval)
		go gc.Clean(st, hi, time.Tick(1e9))
		var m consensus.Manager
		m.Self = self
		m.DefRev = start
		m.Alpha = alpha
		m.In = in
		m.Out = out
		m.Ops = st.Ops
		m.PSeqn = pr.seqns
		m.Props = pr.props
		m.TFill = fillDelay
		m.Store = st
		m.Ticker = time.Tick(10e6)
		go m.Run()
	}

	if cl == nil { // we are the only node in a new cluster
		if st.Journal != nil {
			for {
				m, err := st.Journal.ReadMutation()
				if err == io.EOF || err == io.ErrUnexpectedEOF  {
					break
				}
				if err != nil {
					// BUG(aram): need to clean journal, so
					// subsequent writes will be read. Maybe fix
					// in ReadMutation instead?
					break
				}
				path, v, rev, _, err := store.Decode(m)
				if err != nil {
					panic(err)
				}
				set(st, path, v, rev, true)
			}
		}

		set(st, "/ctl/name", clusterName, store.Missing, true)
		set(st, "/ctl/node/"+self+"/addr", listenAddr, store.Missing, true)
		set(st, "/ctl/node/"+self+"/hostname", os.Getenv("HOSTNAME"), store.Missing, true)
		set(st, "/ctl/node/"+self+"/version", Version, store.Missing, true)
		set(st, "/ctl/cal/0", self, store.Missing, true)
		if buri == "" {
			set(st, "/ctl/ns/"+clusterName+"/"+self, listenAddr, store.Missing, true)
		}
		calSrv(<-st.Seqns)
		// Skip ahead alpha steps so that the registrar can provide a
		// meaningful cluster.
		for i := 0; i < alpha; i++ {
			st.Ops <- store.Op{1 + <-st.Seqns, store.Nop, true}
		}
		canWrite <- true
		go setReady(pr, self)
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

		errs := make(chan error)
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
			go setReady(pr, self)
			if buri != "" {
				b, err := doozer.DialUri(buri, "")
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
			n, err := udpConn.WriteTo(p.Data, p.Addr)
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

	selfAddr, ok := udpConn.LocalAddr().(*net.UDPAddr)
	if !ok {
		panic("no UDP addr")
	}
	lv := liveness{
		timeout: kickTimeout,
		ival:    kickTimeout / 2,
		self:    selfAddr,
		shun:    shun,
	}
	for {
		t := time.Now().UnixNano()

		buf := make([]byte, maxUDPLen)
		n, addr, err := udpConn.ReadFromUDP(buf)
		if err == os.EINVAL {
			return
		}
		if err != nil {
			log.Println(err)
			continue
		}

		buf = buf[:n]

		lv.mark(addr, t)
		lv.check(t)

		in <- consensus.Packet{addr, buf}
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
			panic(io.EOF)
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
		} else if ev.IsSet() && ev.Body == self {
			return ev.Seqn
		}
	}

	return 0
}

func advanceUntil(cl *doozer.Conn, ver <-chan int64, done int64) {
	for <-ver < done {
		cl.Nop()
	}
}

func set(st *store.Store, path, body string, rev int64, volatile bool) {
	mut := store.MustEncodeSet(path, body, rev)
	st.Ops <- store.Op{1 + <-st.Seqns, mut, volatile}
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
		st.Ops <- store.Op{ev.Rev, mut, false}
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
	c.ch <- store.Op{f.Rev, mut, false}
}

func setReady(p consensus.Proposer, self string) {
	m, err := store.EncodeSet("/ctl/node/"+self+"/writable", "true", 0)
	if err != nil {
		log.Println(err)
		return
	}
	p.Propose([]byte(m))
}
