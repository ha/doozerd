// Borg

package borg

import (
	"borg/proto"
	"bufio"
	"io"
	"net"
	"fmt"
	"bytes"
)

type Request struct {
	*proto.Request
	Resp chan Response
}

type Response io.Reader

func (r *Request) Respond(parts ... []byte) {
	buf := bytes.NewBufferString("")
	fmt.Fprintf(buf, "*%d\r\n", len(parts))
	for _, part := range(parts) {
		if part == nil {
			buf.WriteString("$-1")
		} else {
			fmt.Fprintf(buf, "$%d\r\n", len(part))
			buf.Write(part)
		}
	}

	r.Resp <-buf
}

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

