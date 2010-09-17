package client

import (
	"junta/proto"
	"net"
	"os"
	"strconv"
)

var ErrInvalidResponse = os.NewError("invalid response")

type Conn interface {
	SendRequest(...string) (uint, os.Error)
	ReadResponse(uint) ([]string, os.Error)
}

type ErrRedirect struct {
	addr string
}

func (e *ErrRedirect) String() string {
	return "redirect to " + e.addr
}

func Dial(addr string) (Conn, os.Error) {
	c, err := net.Dial("tcp", "", addr)
	if err != nil {
		return nil, err
	}
	return proto.NewConn(c), nil
}

func Join(c Conn, id, addr string) (seqn uint64, snapshot string, err os.Error) {
	var rid uint
	rid, err = c.SendRequest("join", id, addr)
	if err != nil {
		return
	}

	var parts []string
	parts, err = c.ReadResponse(rid)
	if err != nil {
		return
	}

	if len(parts) != 2 {
		err = ErrInvalidResponse
		return
	}

	seqn, err = strconv.Btoui64(parts[0], 10)
	if err != nil {
		return
	}

	snapshot = parts[1]
	return
}

func Set(c Conn, path, body, cas string) (seqn uint64, err os.Error) {
	var rid uint
	rid, err = c.SendRequest("set", path, body, cas)
	if err != nil {
		return
	}

	var parts []string
	parts, err = c.ReadResponse(rid)
	if err != nil {
		return
	}

	if len(parts) < 1 {
		err = ErrInvalidResponse
		return
	}

	switch parts[0] {
	case "OK":
		if len(parts) != 2 {
			err = ErrInvalidResponse
			return
		}
		return strconv.Btoui64(parts[1], 10)
	case "redirect":
		if len(parts) != 2 {
			err = ErrInvalidResponse
			return
		}
		err = &ErrRedirect{parts[1]}
		return
	}
	err = ErrInvalidResponse
	return
}

func Del(c Conn, path, cas string) (seqn uint64, err os.Error) {
	var rid uint
	rid, err = c.SendRequest("del", path, cas)
	if err != nil {
		return
	}

	var parts []string
	parts, err = c.ReadResponse(rid)
	if err != nil {
		return
	}

	if len(parts) != 1 {
		err = ErrInvalidResponse
		return
	}

	return strconv.Btoui64(parts[0], 10)
}
