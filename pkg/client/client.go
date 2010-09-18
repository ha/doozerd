package client

import (
	"junta/proto"
	"net"
	"os"
	"strconv"
)

var ErrInvalidResponse = os.NewError("invalid response")

type Conn struct {
    p *proto.Conn
}

func Dial(addr string) (*Conn, os.Error) {
	c, err := net.Dial("tcp", "", addr)
	if err != nil {
		return nil, err
	}
    p := proto.NewConn(c)
	return &Conn{p}, nil
}

func (c *Conn) call(n int, a ...string) (parts []string, err os.Error) {
	var rid uint
	rid, err = c.p.SendRequest(a)
	if err != nil {
		return
	}

	parts, err = c.p.ReadResponse(rid)
	if err != nil {
		return
	}

	if len(parts) != n {
		err = ErrInvalidResponse
		return
	}

	return
}

func (c *Conn) Join(id, addr string) (seqn uint64, snapshot string, err os.Error) {
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

func (c *Conn) Set(path, body, cas string) (seqn uint64, err os.Error) {
	var parts []string
	parts, err = c.call(1, "set", path, body, cas)
	if err != nil {
		return
	}

	return strconv.Btoui64(parts[0], 10)
}

func (c *Conn) Del(path, cas string) (seqn uint64, err os.Error) {
	var parts []string
	parts, err = c.call(1, "del", path, cas)
	if err != nil {
		return
	}

	return strconv.Btoui64(parts[0], 10)
}
