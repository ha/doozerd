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
	pr *proto.Conn
	lg *log.Logger
	lk sync.Mutex
}

func Dial(addr string) (*Client, os.Error) {
	c, err := net.Dial("tcp", "", addr)
	if err != nil {
		return nil, err
	}
	pr := proto.NewConn(c)
	go pr.ReadResponses()
	return &Client{pr: pr, lg: util.NewLogger(addr)}, nil
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
func (cl *Client) proto() (*proto.Conn, os.Error) {
	cl.lk.Lock()
	defer cl.lk.Unlock()

	if cl.pr.RedirectAddr != "" {
		conn, err := net.Dial("tcp", "", cl.pr.RedirectAddr)
		if err != nil {
			return nil, err
		}
		cl.lg = util.NewLogger(cl.pr.RedirectAddr)
		cl.pr = proto.NewConn(conn)
		go cl.pr.ReadResponses()
	}

	return cl.pr, nil
}

func (cl *Client) callWithoutRedirect(verb string, args, slot interface{}) os.Error {
	pr, err := cl.proto()
	if err != nil {
		return err
	}

	req, err := pr.SendRequest(verb, args)
	if err != nil {
		return err
	}

	return req.Get(slot)
}

func (cl *Client) call(verb string, data, slot interface{}) (err os.Error) {
	for err = os.EAGAIN; err == os.EAGAIN; {
		err = cl.callWithoutRedirect(verb, data, slot)
	}

	if err != nil {
		cl.lg.Println(err)
	}

	return err
}

func (cl *Client) Join(id, addr string) (seqn uint64, snapshot string, err os.Error) {
	var res proto.ResJoin
	err = cl.call("join", proto.ReqJoin{id, addr}, &res)
	if err != nil {
		return
	}

	return res.Seqn, res.Snapshot, nil
}

func (cl *Client) Set(path, body, oldCas string) (newCas string, err os.Error) {
	err = cl.call("SET", proto.ReqSet{path, body, oldCas}, &newCas)
	return
}

func (cl *Client) Del(path, cas string) os.Error {
	return cl.call("DEL", proto.ReqDel{path, cas}, nil)
}

func (cl *Client) Noop() os.Error {
	return cl.call("NOOP", nil, nil)
}

func (cl *Client) Checkin(id, cas string) (int64, string, os.Error) {
	var res proto.ResCheckin
	err := cl.call("checkin", proto.ReqCheckin{id, cas}, &res)
	if err != nil {
		return 0, "", err
	}

	return res.T, res.Cas, nil
}

func (cl *Client) Sett(path string, n int64, cas string) (int64, string, os.Error) {
	var res proto.ResSett
	err := cl.call("SETT", proto.ReqSett{path, n, cas}, &res)
	if err != nil {
		return 0, "", err
	}

	return res.T, res.Cas, nil
}
