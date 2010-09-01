package proto

import (
	"os"
	"io"
	"fmt"
	"strings"
	"strconv"
	"net/textproto"
)

type Conn struct {
	*textproto.Conn
}

func NewConn(conn io.ReadWriteCloser) (*Conn) {
	return &Conn{textproto.NewConn(conn)}
}

func (c *Conn) Call(parts ... string) (uint, os.Error) {
	id := c.Next()
	c.StartRequest(id)
	err := encode(&c.Writer, parts)
	c.EndRequest(id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (c *Conn) ReadParts(id uint) ([]string, os.Error) {
	c.StartResponse(id)
	defer c.EndResponse(id)
	return decode(&c.Reader)
}

func decode(r *textproto.Reader) (parts []string, err os.Error) {
	var count int = 1
	var size int
	var line string

Loop:
	for count > 0 {
		// TODO: test if len(line) == 0
		line, err = r.ReadLine()
		switch {
		case err == os.EOF: break Loop
		case err != nil: panic(err)
		}
		line = strings.TrimSpace(line)
		if len(line) < 1 {
			continue Loop
		}
		switch line[0] {
		case '*':
			count, _ = strconv.Atoi(line[1:])
			parts = make([]string, count)
		case '$':
			// TODO: test for err
			size, _ = strconv.Atoi(line[1:])
			buf := make([]byte, size)
			// TODO: test for err
			n, err := io.ReadFull(r.R, buf)
			switch {
			case n != size: panic(fmt.Sprintf("n:%d\n", n))
			case err != nil: panic(err)
			}
			parts[len(parts) - count] = string(buf)
			count--
		}
	}
	return
}

func encode(w *textproto.Writer, parts ... string) (err os.Error) {
	if err = w.PrintfLine("*%d", len(parts)); err != nil {
		return
	}
	for _, part := range parts {
		if err = w.PrintfLine("$%d", len(part)); err != nil {
			return
		}
		if err = w.PrintfLine("%s", part); err != nil {
			return
		}
	}
	return nil
}
