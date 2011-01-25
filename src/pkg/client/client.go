package client

import (
	"doozer/proto"
	"doozer/util"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"io"
	"os"
	pb "goprotobuf.googlecode.com/hg/proto"
	"sync"
)

const (
	Valid = 1 << iota
	Done
)

var lg = util.NewLogger("client")

var (
	ErrNoAddrs = os.NewError("no known address")
)

var (
	cancel  = proto.NewRequest_Verb(proto.Request_CANCEL)
	checkin = proto.NewRequest_Verb(proto.Request_CHECKIN)
	del     = proto.NewRequest_Verb(proto.Request_DEL)
	delsnap = proto.NewRequest_Verb(proto.Request_DELSNAP)
	get     = proto.NewRequest_Verb(proto.Request_GET)
	join    = proto.NewRequest_Verb(proto.Request_JOIN)
	noop    = proto.NewRequest_Verb(proto.Request_NOOP)
	set     = proto.NewRequest_Verb(proto.Request_SET)
	snap    = proto.NewRequest_Verb(proto.Request_SNAP)
	walk    = proto.NewRequest_Verb(proto.Request_WALK)
	watch   = proto.NewRequest_Verb(proto.Request_WATCH)
)


type ResponseError struct {
	Code   int32
	Detail string
}


func (r *ResponseError) String() string {
	return "response: " + proto.Response_Err_name[r.Code] + ": " + r.Detail
}


// Response errors
var (
	ErrNotDir      = &ResponseError{proto.Response_NOTDIR, "not a directory"}
	ErrIsDir       = &ResponseError{proto.Response_ISDIR, "is a directory"}
	ErrCasMismatch = &ResponseError{proto.Response_CAS_MISMATCH, "cas mismatch"}
	ErrInvalidSnap = &ResponseError{proto.Response_INVALID_SNAP, "invalid snapshot id"}
	respErrors = map[int32]*ResponseError{
		proto.Response_NOTDIR:       ErrNotDir,
		proto.Response_ISDIR:        ErrIsDir,
		proto.Response_CAS_MISMATCH: ErrCasMismatch,
		proto.Response_INVALID_SNAP: ErrInvalidSnap,
	}
)


type Event struct {
	Cas  int64
	Path string
	Body []byte
	Err  os.Error
}


type T proto.Request

type R proto.Response


func (r *R) err() os.Error {
	if r == nil {
		return os.EOF
	}

	if r.ErrCode != nil {
		c := int32(*r.ErrCode)

		if r.ErrDetail != nil {
			return &ResponseError{c, *r.ErrDetail}
		}

		if r, ok := respErrors[c]; ok {
			return r
		}

		return &ResponseError{c, proto.Response_Err_name[c]}
	}
	return nil
}


func (r *R) String() string {
	return fmt.Sprintf("%#v", r)
}


type conn struct {
	addr string
	c    net.Conn
	clk  sync.Mutex // write lock
	err  os.Error   // unrecoverable error

	// callback management
	n    int32             // next tag
	cb   map[int32]chan *R // callback channels
	cblk sync.Mutex

	lg *log.Logger

	// redirect handling
	redirectAddr string
	redirected   bool

	closed chan bool
}


func (c *conn) writeT(t *T) os.Error {
	if c.err != nil {
		return c.err
	}

	buf, err := pb.Marshal(t)
	if err != nil {
		return err
	}

	c.err = binary.Write(c.c, binary.BigEndian, int32(len(buf)))
	if c.err != nil {
		return c.err
	}

	for len(buf) > 0 {
		n, err := c.c.Write(buf)
		if err != nil {
			c.err = err
			return err
		}

		buf = buf[n:]
	}

	return nil
}


func (c *conn) send(t *T) (chan *R, os.Error) {
	if c.err != nil {
		return nil, c.err
	}

	ch := make(chan *R)

	// Find an unused tag and
	// put the reply chan in the table.
	c.cblk.Lock()
	for _, ok := c.cb[c.n]; ok; _, ok = c.cb[c.n] {
		c.n++
	}
	tag := c.n
	c.cb[tag] = ch
	c.cblk.Unlock()

	t.Tag = &tag

	c.clk.Lock()
	err := c.writeT(t)
	c.clk.Unlock()

	if err != nil {
		c.cblk.Lock()
		c.cb[tag] = nil, false
		c.cblk.Unlock()
		return nil, err
	}

	return ch, nil
}


func (c *conn) call(t *T) (*R, os.Error) {
	ch, err := c.send(t)
	if err != nil {
		return nil, err
	}

	r := <-ch
	if err := r.err(); err != nil {
		return nil, err
	}

	return r, nil
}


func (c *conn) events(t *T) (*Watch, os.Error) {
	ch, err := c.send(t)
	if err != nil {
		return nil, err
	}

	evs := make(chan *Event)
	w := &Watch{evs, c, *t.Tag}
	go func() {
		for r := range ch {
			var ev Event
			if err := r.err(); err != nil {
				ev.Err = err
			} else {
				ev.Cas = pb.GetInt64(r.Cas)
				ev.Path = pb.GetString(r.Path)
				ev.Body = r.Value
			}
			evs <- &ev
		}
		close(evs)
	}()

	return w, nil
}


func (c *conn) readR() (*R, os.Error) {
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

	var r R
	err = pb.Unmarshal(buf, &r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}


func (c *conn) close() {
	c.cblk.Lock()
	for _, ch := range c.cb {
		close(ch)
	}
	c.cb = nil
	c.cblk.Unlock()
	c.closed <- true
}


func (c *conn) readResponses() {
	defer c.close()

	for {
		r, err := c.readR()
		if err != nil {
			c.clk.Lock()
			if c.err == nil {
				c.err = err
			}
			c.clk.Unlock()
			return
		}

		if r.ErrCode != nil && *r.ErrCode == proto.Response_REDIRECT {
			c.redirectAddr = pb.GetString(r.ErrDetail)
			c.redirected = true
		}

		tag := pb.GetInt32(r.Tag)
		flags := pb.GetInt32(r.Flags)

		c.cblk.Lock()
		ch, ok := c.cb[tag]
		if flags&Done != 0 {
			c.cb[tag] = nil, false
		}
		c.cblk.Unlock()

		if !ok {
			c.lg.Println("unexpected:", r.String())
			continue
		}

		if flags&Valid != 0 {
			ch <- r
		}

		if flags&Done != 0 {
			close(ch)
		}
	}
}


func (c *conn) cancel(tag int32) os.Error {
	_, err := c.call(&T{Verb: cancel, Id: &tag})
	if err != nil {
		return err
	}

	c.cblk.Lock()
	ch, ok := c.cb[tag]
	if ok {
		c.cb[tag] = nil, false
	}
	c.cblk.Unlock()

	if ok {
		close(ch)
	}
	return nil
}


func (c *conn) monitorAddrs(cl *Client) {
	addrs := make(map[string]string)
	addAddr := func(p, a string) {
		if len(a) > 0 {
			addrs[p] = a
		}
	}

	addrGlob := pb.String("/doozer/info/*/public-addr")
	watchAddr, err := c.events(&T{Verb: watch, Path: addrGlob})
	if err != nil {
		c.lg.Println(err)
		return
	}

	walkAddr, err := c.events(&T{Verb: walk, Path: addrGlob})
	if err != nil {
		c.lg.Println(err)
		return
	}

	init: for {
		select {
		case ev := <-walkAddr.C:
			if closed(walkAddr.C) {
				break init
			}
			addAddr(ev.Path, string(ev.Body))
		case ev := <-watchAddr.C:
			addAddr(ev.Path, string(ev.Body))
		}
	}


	glob := pb.String("/doozer/slot/*")

	watch, err := c.events(&T{Verb: watch, Path: glob})
	if err != nil {
		c.lg.Println(err)
		return
	}

	walk, err := c.events(&T{Verb: walk, Path: glob})
	if err != nil {
		c.lg.Println(err)
		return
	}

	slots := make(map[string]string)

	for {
		if watchAddr.C == nil && walk.C == nil && watch.C == nil {
			break
		}

		select {
		case ev := <-watchAddr.C:
			if closed(watchAddr.C) {
				watchAddr.C = nil
				break
			}
			addAddr(ev.Path, string(ev.Body))
		case ev := <-walk.C:
			if closed(walk.C) {
				walk.C = nil
				break
			}

			if len(ev.Body) > 0 {
				sid := string(ev.Body)
				path := "/doozer/info/" + sid + "/public-addr"
				addr := addrs[path]
				slots[ev.Path] = addr
				cl.a <- addr
			} else {
				addr := slots[ev.Path]
				slots[ev.Path] = "", false
				cl.r <- addr
			}
		case ev := <-watch.C:
			if closed(watch.C) {
				watch.C = nil
				break
			}

			if len(ev.Body) > 0 {
				sid := string(ev.Body)
				path := "/doozer/info/" + sid + "/public-addr"
				addr := addrs[path]
				slots[ev.Path] = addr
				cl.a <- addr
			} else {
				addr := slots[ev.Path]
				slots[ev.Path] = "", false
				cl.r <- addr
			}
		}
	}
}


type Client struct {
	Name string
	c    chan *conn // current connection
	a    chan string // add address
	r    chan string // remove address
	lg   *log.Logger
	Len  chan int
}


// Name is the name of this cluster.
// Addr is an initial (writable) address to connect to.
func New(name, addr string) *Client {
	c := &Client{
		Name: name,
		c:    make(chan *conn),
		a:    make(chan string),
		r:    make(chan string),
		lg:   util.NewLogger(name),
		Len:  make(chan int),
	}
	go c.run(map[string]bool{addr:true})
	return c
}


func (cl *Client) connect(a map[string]bool) *conn {
	for len(a) > 0 {
		var addr string
		for addr = range a {
			break
		}
		c, err := cl.dial(addr)
		if err == nil {
			return c
		}
		cl.lg.Println(err)
		a[addr] = false, false
	}
	close(cl.c)
	return nil
}


func (cl *Client) run(a map[string]bool) {
	c := cl.connect(a)
	if c == nil {
		return
	}

	for {
		select {
		case cl.Len <- len(a):
			// nothing
		case cl.c <- c:
			// nothing
		case add := <-cl.a:
			a[add] = true
		case rm := <-cl.r:
			a[rm] = false, false
		case <-c.closed:
			a[c.addr] = false, false
			c = cl.connect(a)
			if c == nil {
				return
			}
		}
	}
}


func (cl *Client) dial(addr string) (*conn, os.Error) {
	var c conn
	var err os.Error

	c.addr = addr
	c.c, err = net.Dial("tcp", "", addr)
	if err != nil {
		return nil, err
	}

	c.cb = make(map[int32]chan *R)
	c.lg = util.NewLogger(addr)
	c.closed = make(chan bool, 1)
	go c.readResponses()
	go c.monitorAddrs(cl)
	return &c, nil
}


func (cl *Client) call(t *T) (r *R, err os.Error) {
	c := <-cl.c
	if c == nil {
		return nil, ErrNoAddrs
	}

	return c.call(t)
}


func (cl *Client) retry(t *T) (r *R, err os.Error) {
	for {
		c := <-cl.c
		if c == nil {
			return nil, ErrNoAddrs
		}

		r, err = c.call(t)
		if c.err != nil {
			// connection error? then try again with a new conn
			continue
		}

		// success, or some other error
		return
	}

	panic("not reached")
}


func (cl *Client) Join(id, addr string) (seqn int64, snapshot string, err os.Error) {
	r, err := cl.call(&T{Verb: join, Path: &id, Value: []byte(addr)})
	if err != nil {
		return 0, "", err
	}

	return pb.GetInt64(r.Seqn), string(r.Value), nil
}


func (cl *Client) Set(path string, oldCas int64, body []byte) (newCas int64, err os.Error) {
	r, err := cl.call(&T{Verb: set, Path: &path, Value: body, Cas: &oldCas})
	if err != nil {
		return 0, err
	}

	return pb.GetInt64(r.Cas), nil
}


// Returns the body and CAS token of the file at path.
// If snapId is 0, uses the current state, otherwise,
// snapId must be a value previously returned from Snap.
// If path does not denote a file, returns an error.
func (cl *Client) Get(path string, snapId int32) (body []byte, cas int64, err os.Error) {
	r, err := cl.retry(&T{Verb: get, Path: &path, Id: &snapId})
	if err != nil {
		return nil, 0, err
	}

	return r.Value, pb.GetInt64(r.Cas), nil
}


func (cl *Client) Del(path string, cas int64) os.Error {
	_, err := cl.call(&T{Verb: del, Path: &path, Cas: &cas})
	return err
}


func (cl *Client) Noop() os.Error {
	_, err := cl.call(&T{Verb: noop})
	return err
}


func (cl *Client) Checkin(id string, cas int64) (int64, os.Error) {
	r, err := cl.call(&T{Verb: checkin, Path: &id, Cas: &cas})
	if err != nil {
		return 0, err
	}

	return pb.GetInt64(r.Cas), nil
}


func (cl *Client) Snap() (id int32, ver int64, err os.Error) {
	r, err := cl.call(&T{Verb: snap})
	if err != nil {
		return 0, 0, err
	}

	return pb.GetInt32(r.Id), pb.GetInt64(r.Seqn), nil
}


func (cl *Client) DelSnap(id int32) os.Error {
	_, err := cl.call(&T{Verb: delsnap, Id: &id})
	return err
}


func (cl *Client) Watch(glob string) (*Watch, os.Error) {
	c := <-cl.c
	if c == nil {
		return nil, ErrNoAddrs
	}

	return c.events(&T{Verb: watch, Path: &glob})
}


func (cl *Client) Walk(glob string) (*Watch, os.Error) {
	c := <-cl.c
	if c == nil {
		return nil, ErrNoAddrs
	}

	return c.events(&T{Verb: walk, Path: &glob})
}


type Watch struct {
	C   <-chan *Event // to caller
	c   *conn
	tag int32
}


func (w *Watch) Cancel() os.Error {
	return w.c.cancel(w.tag)
}
