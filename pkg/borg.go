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
	resp chan Response
}

type Response io.Reader

func (r *Request) Respond(parts ... []byte) {
	buf := bytes.NewBufferString("")
	fmt.Fprintf(buf, "*%d\r\n", len(parts))
	for _, part := range(parts) {
		if part == nil {
			buf.WriteString("$-1\r\n")
		} else {
			fmt.Fprintf(buf, "$%d\r\n", len(part))
			buf.Write(part)
		}
	}

	r.resp <-buf
}

func (r *Request) RespondNil() {
	r.Respond(nil)
}

func (r *Request) RespondOk() {
	r.RespondOkMsg("OK")
}

func (r *Request) RespondOkMsg(msg string) {
	r.resp <-bytes.NewBufferString("+" + msg)
}

func (r *Request) RespondErrf(format string, a ... interface {}) {
	buf := bytes.NewBufferString("-ERR: ")
	fmt.Fprintf(buf, format, a)
	buf.WriteString("\r\n")
	r.resp <-buf
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

