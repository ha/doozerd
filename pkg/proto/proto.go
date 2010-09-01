package proto

import (
	"os"
	"io"
	"fmt"
	"strings"
	"strconv"
	"net/textproto"
)

// Operations
const (
	SendReq = "send req"
	SendRes = "send res"
	SendErr = "send err"
	ReadReq = "read req"
	ReadRes = "read res"
)

// Errors we can have
const (
	InvalidCommand = "invalid command"
)

type Conn struct {
	*textproto.Conn
}

type ProtoError struct {
	Id uint
	Op string
	Error os.Error
}

func (e *ProtoError) String() string {
	return fmt.Sprintf("%s %d: %s", e.Op, e.Id, e.Error)
}

type ResponseError string

func (e ResponseError) String() string {
	return string(e)
}

func NewConn(conn io.ReadWriteCloser) (*Conn) {
	return &Conn{textproto.NewConn(conn)}
}

// Server functions

func (c *Conn) SendResponse(id uint, parts ... string) os.Error {
	c.StartResponse(id)
	defer c.EndResponse(id)
	err := encode(&c.Writer, parts)
	if err != nil {
		return &ProtoError{id, SendRes, err}
	}
	return nil
}

func (c *Conn) SendError(id uint, msg string) os.Error {
	c.StartResponse(id)
	defer c.EndResponse(id)
	err := c.PrintfLine("-ERR: %s", msg)
	if err != nil {
		return &ProtoError{id, SendErr, err}
	}
	return nil
}

func (c *Conn) ReadRequest() (uint, []string, os.Error) {
	id := c.Next()
	c.StartRequest(id)
	parts, err := decode(&c.Reader)
	c.EndRequest(id)
	if err != nil {
		if err == os.EOF {
			return 0, nil, err
		} else {
			return 0, nil, &ProtoError{id, ReadReq, err}
		}
	}
	return id, parts, nil
}

// Client functions

func (c *Conn) SendRequest(parts ... string) (uint, os.Error) {
	id := c.Next()
	c.StartRequest(id)
	err := encode(&c.Writer, parts)
	c.EndRequest(id)
	if err != nil {
		return 0, &ProtoError{id, SendReq, err}
	}
	return id, nil
}

func (c *Conn) ReadResponse(id uint) ([]string, os.Error) {
	c.StartResponse(id)
	defer c.EndResponse(id)

	parts, err := decode(&c.Reader)

	switch err.(type) {
	default:
		return nil, &ProtoError{id, ReadRes, err}
	case nil:
		return parts, nil
	case ResponseError:
		return nil, err
	}

	panic("unreachable!")
}

// Helpers

func decode(r *textproto.Reader) (parts []string, err os.Error) {
	var count int = 1
	var size int
	var line string

Loop:
	for count > 0 {
		// TODO: test if len(line) == 0
		line, err = r.ReadLine()
		switch {
		case err == os.EOF: return
		case err != nil: panic(err)
		}
		line = strings.TrimSpace(line)
		if len(line) < 1 {
			continue Loop
		}
		switch line[0] {
		case '-':
			err = ResponseError(line[1:])
			return
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
