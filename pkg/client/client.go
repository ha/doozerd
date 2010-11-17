package client

import (
	"doozer/proto"
	"doozer/util"
	"log"
	"net"
	"os"
	"sync"
)

var ErrInvalidResponse = os.NewError("invalid response")

type Client struct {
	p  *proto.Conn
	lg *log.Logger
	lk sync.Mutex
}

func Dial(addr string) (*Client, os.Error) {
	c, err := net.Dial("tcp", "", addr)
	if err != nil {
		return nil, err
	}
	p := proto.NewConn(c)
	go p.ReadResponses()
	return &Client{p: p, lg: util.NewLogger(addr)}, nil
}

// This is a little subtle. We want to follow redirects while still pipelining
// requests, and we want to allow as many requests as possible to succeed
// without retrying unnecessarily.
//
// In particular, reads never need to redirect, and writes must always go to
// the leader. So we want that read requests never retry, and write requests
// retry if and only if necessary. Here's how it works:
//
// In the proto.Conn, when we get a redirect response, we raise a flag noting
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
func (c *Client) proto() (*proto.Conn, os.Error) {
	c.lk.Lock()
	defer c.lk.Unlock()

	if c.p.RedirectAddr != "" {
		conn, err := net.Dial("tcp", "", c.p.RedirectAddr)
		if err != nil {
			return nil, err
		}
		c.lg = util.NewLogger(c.p.RedirectAddr)
		c.p = proto.NewConn(conn)
		go c.p.ReadResponses()
	}

	return c.p, nil
}

func (c *Client) callWithoutRedirect(verb string, a, slot interface{}) os.Error {
	p, err := c.proto()
	if err != nil {
		return err
	}

	r, err := p.SendRequest(verb, a)
	if err != nil {
		return err
	}

	return r.Get(slot)
}

func (c *Client) call(verb string, data, slot interface{}) (err os.Error) {
	for err = os.EAGAIN; err == os.EAGAIN; {
		err = c.callWithoutRedirect(verb, data, slot)
	}
	if err != nil {
		c.lg.Println(err)
	}

	return err
}

func (c *Client) Join(id, addr string) (seqn uint64, snapshot string, err os.Error) {
	var r proto.ResJoin
	err = c.call("join", proto.ReqJoin{id, addr}, &r)
	if err != nil {
		return
	}

	return r.Seqn, r.Snapshot, nil
}

func (c *Client) Set(path, body, cas string) (seqn uint64, err os.Error) {
	err = c.call("set", proto.ReqSet{path, body, cas}, &seqn)
	return
}

func (c *Client) Del(path, cas string) (seqn uint64, err os.Error) {
	err = c.call("del", proto.ReqDel{path, cas}, &seqn)
	return
}

func (c *Client) Nop() os.Error {
	return c.call("nop", nil, nil)
}

func (c *Client) Checkin(id, cas string) (t int64, ncas string, err os.Error) {
	var r proto.ResCheckin
	err = c.call("checkin", proto.ReqCheckin{id, cas}, &r)
	if err != nil {
		return
	}

	return r.T, r.Cas, nil
}
