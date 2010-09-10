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
