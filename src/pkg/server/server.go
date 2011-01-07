package server

import (
	dnet "doozer/net"
	"doozer/paxos"
	"doozer/proto"
	"doozer/store"
	"doozer/util"
	"encoding/binary"
	"io"
	"net"
	"os"
	pb "goprotobuf.googlecode.com/hg/proto"
	"rand"
	"strconv"
	"sync"
	"time"
)


const packetSize = 3000


const (
	sessionLease = 60e9 // ns == 60s
	sessionPad   = 5e8 // ns == .5s
)


var (
	ErrPoisoned = os.NewError("poisoned")
)


var (
	other       = proto.NewResponse_Err(proto.Response_OTHER)
	redirect    = proto.NewResponse_Err(proto.Response_REDIRECT)
	newErrCode  = proto.NewResponse_Err
	tagInUse    = &R{ErrCode: newErrCode(proto.Response_TAG_IN_USE)}
	isDir       = &R{ErrCode: newErrCode(proto.Response_ISDIR)}
	badSnap     = &R{ErrCode: newErrCode(proto.Response_INVALID_SNAP)}
	readonly    = &R{
		ErrCode: newErrCode(proto.Response_OTHER),
		ErrDetail: pb.String("no known writeable addresses"),
	}
	badTag      = &R{
		ErrCode: newErrCode(proto.Response_OTHER),
		ErrDetail: pb.String("unknown tag"),
	}
)


func errResponse(e os.Error) *R {
	return &R{ErrCode: other, ErrDetail: pb.String(e.String())}
}


// Response flags
const (
	Valid = 1 << iota
	Done
)


var slots = store.MustCompileGlob("/doozer/slot/*")


type T proto.Request
type R proto.Response


type OpError struct {
	Detail string
}


type Manager interface {
	paxos.Proposer
	ProposeOnce(v string) store.Event
	PutFrom(string, paxos.Msg)
	Alpha() int
}


type Server struct {
	Conn net.PacketConn
	Addr string
	St   *store.Store
	Mg   Manager
	Self string
}


func (sv *Server) ServeUdp(outs chan paxos.Packet) {
	r := dnet.Ackify(sv.Conn, outs)

	for p := range r {
		sv.Mg.PutFrom(p.Addr, p.Msg)
	}
}


var clg = util.NewLogger("cal")


func (s *Server) Serve(l net.Listener, cal chan int) os.Error {
	for {
		rw, err := l.Accept()
		if err != nil {
			if e, ok := err.(*net.OpError); ok && e.Error == os.EINVAL {
				return nil
			}
			return err
		}
		c := &conn{
			c:       rw,
			s:       s,
			cal:     closed(cal),
			snaps:   make(map[int32]store.Getter),
			cancels: make(map[int32]chan int),
		}
		go c.serve()
	}

	panic("unreachable")
}


func (sv *Server) cals() []string {
	cals := make([]string, 0)
	_, g := sv.St.Snap()
	store.Walk(g, slots, func(_, body, _ string) {
		if len(body) > 0 {
			cals = append(cals, body)
		}
	})
	return cals
}


// Repeatedly propose nop values until a successful read from `done`.
func (sv *Server) AdvanceUntil(done chan int) {
	for _, ok := <-done; !ok; _, ok = <-done {
		sv.Mg.Propose(store.Nop)
	}
}


type conn struct {
	c        net.Conn
	s        *Server
	cal      bool
	sid      int32
	snaps    map[int32]store.Getter
	slk      sync.RWMutex
	cancels  map[int32]chan int
	wl       sync.Mutex // write lock
	poisoned bool
}


func (c *conn) readBuf() (*T, os.Error) {
	var size int32
	err := binary.Read(c.c, binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	_, err = io.ReadFull(c.c, buf)
	if err != nil {
		return nil, err
	}

	var t T
	err = pb.Unmarshal(buf, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}


func (c *conn) makeCancel(t *T) chan int {
	tag := pb.GetInt32(t.Tag)

	c.wl.Lock()
	defer c.wl.Unlock()

	if _, ok := c.cancels[tag]; ok {
		return nil
	}

	ch := make(chan int)
	c.cancels[tag] = ch
	return ch
}


func (c *conn) cancellable(t *T, f func() *R) *R {
	ch := make(chan *R, 1)
	cancel := c.makeCancel(t)
	if cancel == nil {
		return tagInUse
	}

	go func() { ch <- f() }()

	go func() {
		select {
		case r := <-ch:
			c.respond(t, Valid|Done, r)
		case <-cancel:
		}
	}()

	return nil
}


func (c *conn) respond(t *T, flag int32, r *R) os.Error {
	r.Tag = t.Tag
	r.Flags = pb.Int32(flag)
	tag := pb.GetInt32(t.Tag)

	c.wl.Lock()
	defer c.wl.Unlock()

	if c.poisoned {
		return ErrPoisoned
	}

	if ch := c.cancels[tag]; ch != nil && flag&Done != 0 {
		c.cancels[tag] = nil, false
		close(ch)
	}

	buf, err := pb.Marshal(r)
	if err != nil {
		c.poisoned = true
		return err
	}

	err = binary.Write(c.c, binary.BigEndian, int32(len(buf)))
	if err != nil {
		c.poisoned = true
		return err
	}

	for len(buf) > 0 {
		n, err := c.c.Write(buf)
		if err != nil {
			c.poisoned = true
			return err
		}

		buf = buf[n:]
	}

	return nil
}


func (c *conn) redirect() *R {
	cals := c.s.cals()
	if len(cals) < 1 {
		return readonly
	}

	cal := cals[rand.Intn(len(cals))]
	parts, cas := c.s.St.Get("/doozer/info/" + cal + "/public-addr")
	if cas == store.Dir && cas == store.Missing {
		return readonly
	}

	return &R{ErrCode: redirect, ErrDetail: &parts[0]}
}


func (c *conn) get(t *T) *R {
	var g store.Getter
	id := pb.GetInt32(t.Id)
	if id == 0 {
		g = c.s.St
	} else {
		var ok bool
		c.slk.RLock()
		g, ok = c.snaps[id]
		c.slk.RUnlock()
		if !ok {
			return badSnap
		}
	}

	v, cas := g.Get(pb.GetString(t.Path))
	if cas == store.Dir {
		return isDir
	}

	var r R
	r.Cas = &cas
	if len(v) == 1 { // not missing
		r.Value = []byte(v[0])
	}
	return &r
}


func (c *conn) set(t *T) *R {
	if !c.cal {
		return c.redirect()
	}

	return c.cancellable(t, func() *R {
		_, cas, err := paxos.Set(c.s.Mg, *t.Path, string(t.Value), *t.Cas)
		if err != nil {
			return errResponse(err)
		}

		return &R{Cas: &cas}
	})
}


func (c *conn) del(t *T) *R {
	if !c.cal {
		return c.redirect()
	}

	return c.cancellable(t, func() *R {
		err := paxos.Del(c.s.Mg, *t.Path, *t.Cas)
		if err != nil {
			return errResponse(err)
		}

		return &R{}
	})
}


func (c *conn) noop(t *T) *R {
	if !c.cal {
		return c.redirect()
	}

	return c.cancellable(t, func() *R {
		c.s.Mg.ProposeOnce(store.Nop)
		return &R{}
	})
}


func (c *conn) join(t *T) *R {
	if !c.cal {
		return c.redirect()
	}

	return c.cancellable(t, func() *R {
		key := "/doozer/members/" + pb.GetString(t.Path)
		seqn, _, err := paxos.Set(c.s.Mg, key, string(t.Value), store.Missing)
		if err != nil {
			return errResponse(err)
		}

		done := make(chan int)
		go c.s.AdvanceUntil(done)
		c.s.St.Sync(seqn + uint64(c.s.Mg.Alpha()))
		close(done)
		seqn, snap := c.s.St.Snapshot()
		seqn1 := int64(seqn)
		return &R{Seqn: &seqn1, Value: []byte(snap)}
	})
}


func (c *conn) checkin(t *T) *R {
	if !c.cal {
		return c.redirect()
	}

	return c.cancellable(t, func() *R {
		body := strconv.Itoa64(time.Nanoseconds() + sessionLease)
		sess := pb.GetString(t.Path)
		cas := pb.GetString(t.Cas)
		_, cas, err := paxos.Set(c.s.Mg, "/session/"+sess, body, cas)
		if err != nil {
			return errResponse(err)
		}

		time.Sleep(sessionLease - sessionPad)
		return &R{Cas: &cas}
	})
}


func (c *conn) cancel(t *T) *R {
	tag := pb.GetInt32(t.Id)

	c.wl.Lock()
	ch := c.cancels[tag]
	c.wl.Unlock()

	if ch != nil {
		ch <- 1
		close(ch)
	}

	c.wl.Lock()
	c.cancels[tag] = nil, false
	c.wl.Unlock()

	return &R{}
}


func (c *conn) watch(t *T) *R {
	pat := pb.GetString(t.Path)
	glob, err := store.CompileGlob(pat)
	if err != nil {
		return errResponse(err)
	}

	cancel := c.makeCancel(t)
	if cancel == nil {
		return tagInUse
	}

	w := store.NewWatch(c.s.St, glob)

	go func() {
		defer close(cancel)
		defer close(w.C)
		defer w.Stop()

		// TODO buffer (and possibly discard) events
		for {
			select {
			case ev := <-w.C:
				if closed(w.C) {
					return
				}
				var r R
				r.Path = &ev.Path
				r.Value = []byte(ev.Body)
				r.Cas = &ev.Cas
				err := c.respond(t, Valid, &r)
				if err != nil {
					// TODO log error
					return
				}
			case <-cancel:
				return
			}
		}
	}()

	return nil
}


func (c *conn) snap(t *T) *R {
	ver, g := c.s.St.Snap()

	var r R
	r.Seqn = pb.Int64(int64(ver))

	c.slk.Lock()
	c.sid++
	r.Id = pb.Int32(c.sid)
	c.snaps[*r.Id] = g
	c.slk.Unlock()

	return &r
}


func (c *conn) delSnap(t *T) *R {
	id := pb.GetInt32(t.Id)
	c.slk.Lock()
	c.snaps[id] = nil, false
	c.slk.Unlock()

	return &R{}
}


var ops = map[int32] func(*conn, *T) *R {
	proto.Request_CANCEL:  (*conn).cancel,
	proto.Request_DEL:     (*conn).del,
	proto.Request_DELSNAP: (*conn).delSnap,
	proto.Request_NOOP:    (*conn).noop,
	proto.Request_GET:     (*conn).get,
	proto.Request_SET:     (*conn).set,
	proto.Request_SNAP:    (*conn).snap,
	proto.Request_WATCH:   (*conn).watch,
	proto.Request_JOIN:    (*conn).join,
	proto.Request_CHECKIN: (*conn).checkin,
}


func (c *conn) serve() {
	logger := util.NewLogger("%v", c.c.RemoteAddr())
	logger.Println("accepted connection")
	for {
		t, err := c.readBuf()
		if err != nil {
			if err == os.EOF {
				logger.Println("connection closed by peer")
			} else {
				logger.Println(err)
			}
			return
		}

		rlogger := util.NewLogger("%v - req [%d]", c.c.RemoteAddr(), t.Tag)

		verb := pb.GetInt32((*int32)(t.Verb))
		f, ok := ops[verb]
		if !ok {
			rlogger.Printf("unknown verb <%d>", verb)
			var r R
			r.ErrCode = proto.NewResponse_Err(proto.Response_UNKNOWN_VERB)
			c.respond(t, Valid|Done, &r)
			continue
		}

		r := f(c, t)
		if r != nil {
			c.respond(t, Valid|Done, r)
		}
	}
}
