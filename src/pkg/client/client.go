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
	if r.ErrCode != nil {
		c := int32(*r.ErrCode)

		if c == proto.Response_REDIRECT {
			return os.EAGAIN
		}

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
	c   net.Conn
	clk sync.Mutex // write lock
	err os.Error   // unrecoverable error

	// callback management
	n    int32             // next tag
	cb   map[int32]chan *R // callback channels
	cblk sync.Mutex

	lg *log.Logger

	// redirect handling
	redirectAddr string
	redirected   bool
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


func (c *conn) readResponses() {
	for {
		r, err := c.readR()
		if err != nil {
			c.lg.Println(err)
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
		if ok && flags&Done != 0 {
			c.cb[tag] = nil, false
		}
		c.cblk.Unlock()

		if ok {
			if flags&Valid != 0 {
				ch <- r
			}

			if flags&Done != 0 {
				close(ch)
			}
		} else {
			c.lg.Println("unexpected:", r.String())
		}
	}
}


func (c *conn) cancel(tag int32) os.Error {
	verb := proto.NewRequest_Verb(proto.Request_CANCEL)
	_, err := c.call(&T{Verb: verb, Id: &tag})
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


type Client struct {
	Name string
	c    *conn     // current connection
	ra    []string // known readable addresses
	wa    []string // known writable address
	lg   *log.Logger
	lk   sync.Mutex
}


// Name is the name of this cluster.
// Addr is an initial readable address to connect to.
func New(name, raddr string) *Client {
	return &Client{Name: name, ra: []string{raddr}}
}


func (c *Client) AddRaddr(a string) {
	c.lk.Lock()
	defer c.lk.Unlock()
	for _, s := range c.ra {
		if s == a {
			return
		}
	}
	c.ra = append(c.ra, a)
}


func (c *Client) AddWaddr(a string) {
	c.lk.Lock()
	defer c.lk.Unlock()
	for _, s := range c.wa {
		if s == a {
			return
		}
	}
	c.wa = append(c.wa, a)
}


func (cl *Client) dial(addr string) (*conn, os.Error) {
	var c conn
	var err os.Error

	c.c, err = net.Dial("tcp", "", addr)
	if err != nil {
		return nil, err
	}

	c.cb = make(map[int32]chan *R)
	c.lg = util.NewLogger(addr)
	go c.readResponses()
	return &c, nil
}


// This is a little subtle. We want to follow redirects while still pipelining
// requests, and we want to allow as many requests as possible to succeed
// without retrying unnecessarily.
//
// In particular, reads never need to redirect, and writes must always go to
// a CAL node. So we want that read requests never retry, and write requests
// retry if and only if necessary. Here's how it works:
//
// In the conn, when we get a redirect response, we raise a flag noting
// the new address. This flag only goes up, never down. This flag effectively
// means the connection is deprecated. Any pending requests can go ahead, but
// new requests should use the new address.
//
// In the Client, when we notice that a redirect has occurred (i.e. the flag is
// set), we establish a new connection to the new address. Calls in the future
// will use the new connection. But we also allow the old connection to
// continue functioning as it was. Any writes on the old connection will retry,
// and then they are guaranteed to pick up the new connection. Any reads on the
// old connection will just succeed directly.
func (cl *Client) conn() (c *conn, err os.Error) {
	cl.lk.Lock()
	defer cl.lk.Unlock()

	if cl.c == nil {
		if len(cl.ra) < 1 {
			return nil, ErrNoAddrs
		}

		cl.c, err = cl.dial(cl.ra[0])
		if err != nil {
			cl.ra = cl.ra[1:]
		}

		return cl.c, err
	}

	if cl.c.redirected {
		cl.AddWaddr(cl.c.redirectAddr)
		if len(cl.wa) < 1 {
			return nil, ErrNoAddrs
		}

		cl.c, err = cl.dial(cl.wa[0])
		if err != nil {
			cl.wa = cl.wa[1:]
		}

		return cl.c, err
	}

	return cl.c, nil
}


func (cl *Client) call(verb int32, t *T) (r *R, err os.Error) {
	t.Verb = proto.NewRequest_Verb(verb)

	for err = os.EAGAIN; err == os.EAGAIN; {
		var c *conn
		c, err = cl.conn()
		if err != nil {
			continue
		}

		r, err = c.call(t)
	}

	if err != nil {
		lg.Println(err)
	}

	return
}


func (cl *Client) Join(id, addr string) (seqn int64, snapshot string, err os.Error) {
	r, err := cl.call(proto.Request_JOIN, &T{Path: &id, Value: []byte(addr)})
	if err != nil {
		return 0, "", err
	}

	return pb.GetInt64(r.Seqn), string(r.Value), nil
}


func (cl *Client) Set(path string, oldCas int64, body []byte) (newCas int64, err os.Error) {
	r, err := cl.call(proto.Request_SET, &T{Path: &path, Value: body, Cas: &oldCas})
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
	r, err := cl.call(proto.Request_GET, &T{Path: &path, Id: &snapId})
	if err != nil {
		return nil, 0, err
	}

	return r.Value, pb.GetInt64(r.Cas), nil
}


func (cl *Client) Del(path string, cas int64) os.Error {
	_, err := cl.call(proto.Request_DEL, &T{Path: &path, Cas: &cas})
	return err
}


func (cl *Client) Noop() os.Error {
	_, err := cl.call(proto.Request_NOOP, &T{})
	return err
}


func (cl *Client) Checkin(id string, cas int64) (int64, os.Error) {
	r, err := cl.call(proto.Request_CHECKIN, &T{Path: &id, Cas: &cas})
	if err != nil {
		return 0, err
	}

	return pb.GetInt64(r.Cas), nil
}


func (cl *Client) Snap() (id int32, ver int64, err os.Error) {
	r, err := cl.call(proto.Request_SNAP, &T{})
	if err != nil {
		return 0, 0, err
	}

	return pb.GetInt32(r.Id), pb.GetInt64(r.Seqn), nil
}


func (cl *Client) DelSnap(id int32) os.Error {
	_, err := cl.call(proto.Request_DELSNAP, &T{Id: &id})
	return err
}


func (cl *Client) events(verb int32, glob string) (*Watch, os.Error) {
	c, err := cl.conn()
	if err != nil {
		return nil, err
	}

	var t T
	t.Verb = proto.NewRequest_Verb(verb)
	t.Path = &glob
	ch, err := c.send(&t)
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


func (cl *Client) Watch(glob string) (*Watch, os.Error) {
	return cl.events(proto.Request_WATCH, glob)
}


func (cl *Client) Walk(glob string) (*Watch, os.Error) {
	return cl.events(proto.Request_WALK, glob)
}


type Watch struct {
	C   <-chan *Event // to caller
	c   *conn
	tag int32
}


func (w *Watch) Cancel() os.Error {
	return w.c.cancel(w.tag)
}
