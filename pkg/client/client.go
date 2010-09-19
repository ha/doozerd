package client

import (
	"junta/proto"
	"junta/util"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
)

var ErrInvalidResponse = os.NewError("invalid response")

type Client struct {
    p *proto.Conn
	lg *log.Logger
	lk sync.Mutex
}

func Dial(addr string) (*Client, os.Error) {
	c, err := net.Dial("tcp", "", addr)
	if err != nil {
		return nil, err
	}
    p := proto.NewConn(c)
	return &Client{p:p, lg:util.NewLogger(addr)}, nil
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
// new requests shouldn't use the new address.
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
	}

	return c.p, nil
}

func (c *Client) callWithoutRedirect(a ...string) ([]string, os.Error) {
	p, err := c.proto()
	if err != nil {
		return nil, err
	}

	rid, err := p.SendRequest(a)
	if err != nil {
		return nil, err
	}

	return p.ReadResponse(rid)
}

func (c *Client) call(n int, a ...string) (parts []string, err os.Error) {
	for {
		parts, err = c.callWithoutRedirect(a)
		if r, ok := err.(proto.Redirect); ok {
			c.lg.Log(r)
			continue
		}
		break
	}
	if err != nil {
		c.lg.Log(err)
		return
	}

	if len(parts) != n {
		err = ErrInvalidResponse
		return
	}

	return
}

func (c *Client) Join(id, addr string) (seqn uint64, snapshot string, err os.Error) {
	var parts []string
	parts, err = c.call(2, "join", id, addr)
	if err != nil {
		return
	}

	seqn, err = strconv.Btoui64(parts[0], 10)
	if err != nil {
		return
	}

	snapshot = parts[1]
	return
}

func (c *Client) Set(path, body, cas string) (seqn uint64, err os.Error) {
	var parts []string
	parts, err = c.call(1, "set", path, body, cas)
	if err != nil {
		return
	}

	return strconv.Btoui64(parts[0], 10)
}

func (c *Client) Del(path, cas string) (seqn uint64, err os.Error) {
	var parts []string
	parts, err = c.call(1, "del", path, cas)
	if err != nil {
		return
	}

	return strconv.Btoui64(parts[0], 10)
}
