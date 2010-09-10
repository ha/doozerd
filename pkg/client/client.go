package client

import (
	"junta/proto"
	"net"
	"os"
	"strconv"
)

var ErrInvalidResponse = os.NewError("invalid response")

func Join(id, addr string) (seqn uint64, snapshot string, err os.Error) {
	var c net.Conn
	c, err = net.Dial("tcp", "", addr)
	if err != nil {
		return
	}
	p := proto.NewConn(c)

	var rid uint
	rid, err = p.SendRequest("join", id)
	if err != nil {
		return
	}

	var parts []string
	parts, err = p.ReadResponse(rid)
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
