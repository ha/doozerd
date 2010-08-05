// Borg

package borg

import (
	"borg/proto"
	"bufio"
	"io"
	"net"
)

type Request struct {
	*proto.Request
	resp chan Response
}

type Response io.Reader

func Relay(conn net.Conn, bus chan *Request) {
	reqs := make(chan *proto.Request)
	resps := make(chan Response)
	go proto.Scan(bufio.NewReader(conn), reqs)

	go func() {
		for {
			bus <- &Request{<-reqs, resps}
		}
	}()

	for {
		res := <-resps
		_, err := io.Copy(conn, res)
		if err != nil {
			panic(err)
		}
	}
}

