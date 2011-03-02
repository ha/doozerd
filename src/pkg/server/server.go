package server

import (
	"doozer/consensus"
	"doozer/proto"
	"doozer/store"
	"doozer/util"
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"rand"
	"strconv"
	"sync"
	"time"
	pb "goprotobuf.googlecode.com/hg/proto"
)


const packetSize = 3000


const (
	sessionLease = 6e9 // ns == 6s
	sessionPad   = 3e9 // ns == 3s
)


var (
	ErrPoisoned = os.NewError("poisoned")
)


var (
	badPath     = proto.NewResponse_Err(proto.Response_BAD_PATH)
	missingArg  = &R{ErrCode: proto.NewResponse_Err(proto.Response_MISSING_ARG)}
	tagInUse    = &R{ErrCode: proto.NewResponse_Err(proto.Response_TAG_IN_USE)}
	isDir       = &R{ErrCode: proto.NewResponse_Err(proto.Response_ISDIR)}
	notDir      = &R{ErrCode: proto.NewResponse_Err(proto.Response_NOTDIR)}
	noEnt       = &R{ErrCode: proto.NewResponse_Err(proto.Response_NOENT)}
	badSnap     = &R{ErrCode: proto.NewResponse_Err(proto.Response_INVALID_SNAP)}
	casMismatch = &R{ErrCode: proto.NewResponse_Err(proto.Response_CAS_MISMATCH)}
	readonly    = &R{
		ErrCode:   proto.NewResponse_Err(proto.Response_OTHER),
		ErrDetail: pb.String("no known writeable addresses"),
	}
	badTag = &R{
		ErrCode:   proto.NewResponse_Err(proto.Response_OTHER),
		ErrDetail: pb.String("unknown tag"),
	}
)


func errResponse(e os.Error) *R {
	return &R{
		ErrCode:   proto.NewResponse_Err(proto.Response_OTHER),
		ErrDetail: pb.String(e.String()),
	}
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
	consensus.Proposer
}


type Server struct {
	Addr string
	St   *store.Store
	Mg   Manager
	Self string

	Alpha int64
}


var lg = util.NewLogger("server")


func (s *Server) accept(l net.Listener, ch chan net.Conn) {
	for {
		c, err := l.Accept()
		if err != nil {
			if err == os.EINVAL {
				break
			}
			if e, ok := err.(*net.OpError); ok && e.Error == os.EINVAL {
				break
			}
			lg.Println(err)
		}
		ch <- c
	}
	close(ch)
}


func (s *Server) Serve(l net.Listener, cal chan bool) {
	var w bool
	conns := make(chan net.Conn)
	go s.accept(l, conns)
	for {
		select {
		case rw := <-conns:
			if closed(conns) {
				return
			}
			c := &conn{
				c:     rw,
				addr:  rw.RemoteAddr().String(),
				s:     s,
				cal:   w,
				snaps: make(map[int32]store.Getter),
				tx:    make(map[int32]txn),
			}
			c.log = util.NewLogger("%v", c.addr)
			go c.serve()
		case <-cal:
			cal = nil
			w = true
		}
	}
}


func (sv *Server) cals() []string {
	cals := make([]string, 0)
	_, g := sv.St.Snap()
	store.Walk(g, slots, func(_, body string, _ int64) bool {
		if len(body) > 0 {
			cals = append(cals, body)
		}
		return false
	})
	return cals
}


// Repeatedly propose nop values until a successful read from `done`.
func (sv *Server) AdvanceUntil(done chan int) {
	for {
		select {
		case <-done:
			return
		default:
		}

		sv.Mg.Propose([]byte(store.Nop))
	}
}


func bgSet(p consensus.Proposer, k string, v []byte, c int64) chan store.Event {
	ch := make(chan store.Event)
	go func() {
		ch <- consensus.Set(p, k, v, c)
	}()
	return ch
}


func bgDel(p consensus.Proposer, k string, c int64) chan store.Event {
	ch := make(chan store.Event)
	go func() {
		ch <- consensus.Del(p, k, c)
	}()
	return ch
}


func bgNop(p consensus.Proposer) chan store.Event {
	ch := make(chan store.Event)
	go func() {
		ch <- p.Propose([]byte(store.Nop))
	}()
	return ch
}


type conn struct {
	c        io.ReadWriter
	wl       sync.Mutex // write lock
	addr     string
	s        *Server
	cal      bool
	sid      int32
	snaps    map[int32]store.Getter
	slk      sync.RWMutex
	tx       map[int32]txn
	tl       sync.Mutex // tx lock
	poisoned bool
	log      *log.Logger
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


func (c *conn) respond(t *T, flag int32, cc chan bool, r *R) {
	r.Tag = t.Tag
	r.Flags = pb.Int32(flag)
	tag := pb.GetInt32(t.Tag)

	if flag&Done != 0 {
		c.closeTxn(tag)
	}

	if c.poisoned {
		select {
		case cc <- true:
		default:
		}
		c.log.Println("poisoned")
		return
	}

	buf, err := pb.Marshal(r)
	c.wl.Lock()
	defer c.wl.Unlock()
	if err != nil {
		c.poisoned = true
		select {
		case cc <- true:
		default:
		}
		c.log.Println(err)
		return
	}

	err = binary.Write(c.c, binary.BigEndian, int32(len(buf)))
	if err != nil {
		c.poisoned = true
		select {
		case cc <- true:
		default:
		}
		c.log.Println(err)
		return
	}

	for len(buf) > 0 {
		n, err := c.c.Write(buf)
		if err != nil {
			c.poisoned = true
			select {
			case cc <- true:
			default:
			}
			c.log.Println(err)
			return
		}

		buf = buf[n:]
	}
}


func (c *conn) redirect(t *T) {
	cals := c.s.cals()
	if len(cals) < 1 {
		c.respond(t, Valid|Done, nil, readonly)
		return
	}

	cal := cals[rand.Intn(len(cals))]
	parts, cas := c.s.St.Get("/doozer/info/" + cal + "/public-addr")
	if cas == store.Dir && cas == store.Missing {
		c.respond(t, Valid|Done, nil, readonly)
		return
	}

	r := &R{
		ErrCode:   proto.NewResponse_Err(proto.Response_REDIRECT),
		ErrDetail: &parts[0],
	}
	c.respond(t, Valid|Done, nil, r)
}


func (c *conn) getSnap(id int32) (g store.Getter) {
	if id == 0 {
		return c.s.St
	}

	var ok bool
	c.slk.RLock()
	g, ok = c.snaps[id]
	c.slk.RUnlock()
	if !ok {
		return nil
	}
	return g
}


func (c *conn) get(t *T, tx txn) {
	g := c.getSnap(pb.GetInt32(t.Id))
	if g == nil {
		c.respond(t, Valid|Done, nil, badSnap)
		return
	}

	v, cas := g.Get(pb.GetString(t.Path))
	if cas == store.Dir {
		c.respond(t, Valid|Done, nil, isDir)
		return
	}

	var r R
	r.Cas = &cas
	if len(v) == 1 { // not missing
		r.Value = []byte(v[0])
	}
	c.respond(t, Valid|Done, nil, &r)
}


func (c *conn) set(t *T, tx txn) {
	if !c.cal {
		c.redirect(t)
		return
	}

	if t.Path == nil || t.Cas == nil {
		c.respond(t, Valid|Done, nil, missingArg)
		return
	}

	go func() {
		select {
		case <-tx.cancel:
			c.closeTxn(*t.Tag)
			return
		case ev := <-bgSet(c.s.Mg, *t.Path, t.Value, *t.Cas):
			switch e := ev.Err.(type) {
			case *store.BadPathError:
				c.respond(t, Valid|Done, nil, &R{ErrCode: badPath, ErrDetail: &e.Path})
				return
			}

			switch ev.Err {
			default:
				c.respond(t, Valid|Done, nil, errResponse(ev.Err))
				return
			case store.ErrCasMismatch:
				c.respond(t, Valid|Done, nil, casMismatch)
				return
			case nil:
				c.respond(t, Valid|Done, nil, &R{Cas: &ev.Cas})
				return
			}
		}

		panic("not reached")
	}()
}


func (c *conn) del(t *T, tx txn) {
	if !c.cal {
		c.redirect(t)
		return
	}

	if t.Path == nil || t.Cas == nil {
		c.respond(t, Valid|Done, nil, missingArg)
		return
	}

	go func() {
		select {
		case <-tx.cancel:
			c.closeTxn(*t.Tag)
			return
		case ev := <-bgDel(c.s.Mg, *t.Path, *t.Cas):
			if ev.Err != nil {
				c.respond(t, Valid|Done, nil, errResponse(ev.Err))
				return
			}
		}
		c.respond(t, Valid|Done, nil, &R{})
	}()
}


func (c *conn) noop(t *T, tx txn) {
	if !c.cal {
		c.redirect(t)
		return
	}

	go func() {
		select {
		case <-tx.cancel:
			c.closeTxn(*t.Tag)
			return
		case <-bgNop(c.s.Mg):
		}
		c.respond(t, Valid|Done, nil, &R{})
		return
	}()
}


func (c *conn) monitor(t *T, tx txn) {
	pat := pb.GetString(t.Path)
	glob, err := store.CompileGlob(pat)
	if err != nil {
		c.respond(t, Valid|Done, nil, errResponse(err))
		return
	}

	w := store.NewWatch(c.s.St, glob)
	rev, g := c.s.St.Snap()

	go func() {
		defer w.Stop()
		defer c.closeTxn(*t.Tag)

		var r R
		stopped := store.Walk(g, glob, func(path, body string, cas int64) (stop bool) {
			select {
			case <-tx.cancel:
				return true
			default:
			}

			r.Cas = &cas
			r.Path = &path
			r.Value = []byte(body)
			c.respond(t, Valid, tx.cancel, &r)
			return false
		})
		if stopped {
			return
		}

		// TODO buffer (and possibly discard) events
		for {
			select {
			case <-tx.cancel:
				return
			case ev := <-w.C:
				if ev.Seqn <= rev {
					continue
				}

				var r R
				r.Rev = &ev.Seqn
				r.Cas = &ev.Cas
				r.Path = &ev.Path
				r.Value = []byte(ev.Body)
				c.respond(t, Valid, tx.cancel, &r)
			}
		}
	}()
}


func (c *conn) checkin(t *T, tx txn) {
	if !c.cal {
		c.redirect(t)
		return
	}

	if t.Path == nil || t.Cas == nil {
		c.respond(t, Valid|Done, nil, missingArg)
		return
	}

	go func() {
		deadline := time.Nanoseconds() + sessionLease
		body := strconv.Itoa64(deadline)
		cas := *t.Cas
		path := "/session/" + *t.Path
		if cas != 0 {
			_, cas = c.s.St.Get(path)
			if cas == 0 {
				c.respond(t, Valid|Done, nil, casMismatch)
				return
			}
		}
		select {
		case <-tx.cancel:
			c.closeTxn(*t.Tag)
			return
		case ev := <-bgSet(c.s.Mg, path, []byte(body), cas):
			switch {
			case ev.Err == store.ErrCasMismatch:
				c.respond(t, Valid|Done, nil, casMismatch)
				return
			case ev.Err != nil:
				c.respond(t, Valid|Done, nil, errResponse(ev.Err))
				return
			}

			if *t.Cas != 0 {
				select {
				case <-time.After(deadline - sessionPad - time.Nanoseconds()):
					// nothing
				case <-tx.cancel:
					c.closeTxn(*t.Tag)
					return
				}
			}
		}

		c.respond(t, Valid|Done, nil, &R{Cas: pb.Int64(-1)})
	}()
}


func (c *conn) stat(t *T, tx txn) {
	g := c.getSnap(pb.GetInt32(t.Id))
	if g == nil {
		c.respond(t, Valid|Done, nil, badSnap)
		return
	}

	ln, cas := g.Stat(pb.GetString(t.Path))
	c.respond(t, Valid|Done, nil, &R{Len: &ln, Cas: &cas})
}


func (c *conn) getdir(t *T, tx txn) {
	path := pb.GetString(t.Path)

	g := c.getSnap(pb.GetInt32(t.Id))
	if g == nil {
		c.respond(t, Valid|Done, nil, badSnap)
		return
	}

	go func() {
		ents, cas := g.Get(path)

		if cas == store.Missing {
			c.respond(t, Valid|Done, nil, noEnt)
			return
		}

		if cas != store.Dir {
			c.respond(t, Valid|Done, nil, notDir)
			return
		}

		offset := int(pb.GetInt32(t.Offset))
		limit := int(pb.GetInt32(t.Limit))

		if limit <= 0 {
			limit = len(ents)
		}

		if offset < 0 {
			offset = 0
		}

		end := offset + limit
		if end > len(ents) {
			end = len(ents)
		}

		for _, e := range ents[offset:end] {
			select {
			case <-tx.cancel:
				c.closeTxn(*t.Tag)
				return
			default:
			}

			c.respond(t, Valid, tx.cancel, &R{Path: &e})
		}

		c.respond(t, Done, nil, &R{})
	}()
}


func (c *conn) cancel(t *T, tx txn) {
	tag := pb.GetInt32(t.Id)
	c.tl.Lock()
	otx, ok := c.tx[tag]
	c.tl.Unlock()
	if ok {
		select {
		case otx.cancel <- true:
		default:
		}
		<-otx.done
		c.respond(t, Valid|Done, nil, &R{})
	} else {
		c.respond(t, Valid|Done, nil, badTag)
	}
}


func (c *conn) watch(t *T, tx txn) {
	pat := pb.GetString(t.Path)
	glob, err := store.CompileGlob(pat)
	if err != nil {
		c.respond(t, Valid|Done, nil, errResponse(err))
		return
	}

	w := store.NewWatch(c.s.St, glob)

	go func() {
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
				r.Rev = &ev.Seqn
				c.respond(t, Valid, tx.cancel, &r)
			case <-tx.cancel:
				c.closeTxn(*t.Tag)
				return
			}
		}
	}()
}


func (c *conn) walk(t *T, tx txn) {
	pat := pb.GetString(t.Path)
	glob, err := store.CompileGlob(pat)
	if err != nil {
		c.respond(t, Valid|Done, nil, errResponse(err))
		return
	}

	g := c.getSnap(pb.GetInt32(t.Id))
	if g == nil {
		c.respond(t, Valid|Done, nil, badSnap)
		return
	}

	go func() {
		stopped := store.Walk(c.s.St, glob, func(path, body string, cas int64) (stop bool) {
			select {
			case <-tx.cancel:
				c.closeTxn(*t.Tag)
				return true
			default:
			}

			var r R
			r.Path = &path
			r.Value = []byte(body)
			r.Cas = &cas
			c.respond(t, Valid, tx.cancel, &r)
			return false
		})

		if !stopped {
			c.respond(t, Done, nil, &R{})
		}
	}()
}


func (c *conn) snap(t *T, tx txn) {
	ver, g := c.s.St.Snap()

	var r R
	r.Rev = pb.Int64(int64(ver))

	c.slk.Lock()
	c.sid++
	r.Id = pb.Int32(c.sid)
	c.snaps[*r.Id] = g
	c.slk.Unlock()

	c.respond(t, Valid|Done, nil, &r)
}


func (c *conn) delSnap(t *T, tx txn) {
	if t.Id == nil {
		c.respond(t, Valid|Done, nil, missingArg)
		return
	}

	c.slk.Lock()
	c.snaps[*t.Id] = nil, false
	c.slk.Unlock()

	c.respond(t, Valid|Done, nil, &R{})
}


var ops = map[int32]func(*conn, *T, txn){
	proto.Request_CANCEL:  (*conn).cancel,
	proto.Request_CHECKIN: (*conn).checkin,
	proto.Request_DEL:     (*conn).del,
	proto.Request_DELSNAP: (*conn).delSnap,
	proto.Request_GET:     (*conn).get,
	proto.Request_GETDIR:  (*conn).getdir,
	proto.Request_MONITOR: (*conn).monitor,
	proto.Request_NOOP:    (*conn).noop,
	proto.Request_SET:     (*conn).set,
	proto.Request_SNAP:    (*conn).snap,
	proto.Request_STAT:    (*conn).stat,
	proto.Request_WALK:    (*conn).walk,
	proto.Request_WATCH:   (*conn).watch,
}


func (c *conn) serve() {
	logger := util.NewLogger("%v", c.addr)
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

		rlogger := util.NewLogger("%v - req [%d]", c.addr, t.Tag)

		verb := pb.GetInt32((*int32)(t.Verb))
		f, ok := ops[verb]
		if !ok {
			rlogger.Printf("unknown verb <%d>", verb)
			var r R
			r.ErrCode = proto.NewResponse_Err(proto.Response_UNKNOWN_VERB)
			c.respond(t, Valid|Done, nil, &r)
			continue
		}

		tag := pb.GetInt32((*int32)(t.Tag))
		tx := newTxn()

		c.tl.Lock()
		c.tx[tag] = tx
		c.tl.Unlock()

		f(c, t, tx)
	}
}


func (c *conn) closeTxn(tag int32) {
	c.tl.Lock()
	tx, ok := c.tx[tag]
	c.tx[tag] = txn{}, false
	c.tl.Unlock()
	if ok {
		close(tx.done)
	}
}
