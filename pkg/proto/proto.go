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

type Line string

type Conn struct {
	*textproto.Conn
	RedirectAddr string
}

type ProtoError struct {
	Id    uint
	Op    string
	Error os.Error
}

func (e *ProtoError) String() string {
	return fmt.Sprintf("%s %d: %s", e.Op, e.Id, e.Error)
}

type ResponseError string

func (e ResponseError) String() string {
	return string(e)
}

type Redirect string

func (e Redirect) String() string {
	return "redirect to " + e.Addr()
}

func (e Redirect) Addr() string {
	return string(e)
}

func NewConn(conn io.ReadWriteCloser) *Conn {
	return &Conn{Conn: textproto.NewConn(conn)}
}

// Server functions

func (c *Conn) SendResponse(id uint, data interface{}) os.Error {
	c.StartResponse(id)
	defer c.EndResponse(id)
	err := encode(&c.Writer, data)
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

func (c *Conn) SendRedirect(id uint, addr string) os.Error {
	c.StartResponse(id)
	defer c.EndResponse(id)
	err := c.PrintfLine("-REDIRECT: %s", addr)
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

func (c *Conn) SendRequest(data interface{}) (uint, os.Error) {
	id := c.Next()
	c.StartRequest(id)
	err := encode(&c.Writer, data)
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

	switch terr := err.(type) {
	default:
		return nil, &ProtoError{id, ReadRes, err}
	case nil:
		return parts, nil
	case ResponseError:
		if terr[0:9] == "REDIRECT:" {
			c.RedirectAddr = strings.TrimSpace(string(terr)[10:])
			err = Redirect(c.RedirectAddr)
		}
		return nil, err
	}

	panic("unreachable")
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
		case err == os.EOF:
			return
		case err != nil:
			panic(err)
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
			case n != size:
				panic(fmt.Sprintf("n:%d\n", n))
			case err != nil:
				panic(err)
			}
			parts[len(parts)-count] = string(buf)
			count--
		}
	}
	return
}

func encode(w *textproto.Writer, data interface{}) (err os.Error) {
	switch t := data.(type) {
	default:
		return os.NewError(fmt.Sprintf("unexpected type %T", t))
	case Line:
		if err = w.PrintfLine("+%s", t); err != nil {
			return
		}
	case os.Error:
		if err = w.PrintfLine("-%s", t.String()); err != nil {
			return
		}
	case nil:
		if err = w.PrintfLine("$-1"); err != nil {
			return
		}
	case int:
		return encodeInt64(w, int64(t))
	case int8:
		return encodeInt64(w, int64(t))
	case int16:
		return encodeInt64(w, int64(t))
	case int32:
		return encodeInt64(w, int64(t))
	case int64:
		return encodeInt64(w, t)
	case uint:
		return encodeUint64(w, uint64(t))
	case uint8:
		return encodeUint64(w, uint64(t))
	case uint16:
		return encodeUint64(w, uint64(t))
	case uint32:
		return encodeUint64(w, uint64(t))
	case uint64:
		return encodeUint64(w, t)
	case string:
		return encodeBytes(w, []byte(t))
	case []byte:
		return encodeBytes(w, t)
	case []string:
		// TODO use the builtin append function when it gets released:
		//return encodeSlice(w, append([]interface{}, t...))
		d := make([]interface{}, len(t))
		for i, x := range t {
			d[i] = x
		}
		return encodeSlice(w, d)
	case []interface{}:
		return encodeSlice(w, t)
	}
	return nil
}

func encodeInt64(w *textproto.Writer, data int64) os.Error {
	return w.PrintfLine(":%d", data)
}

func encodeUint64(w *textproto.Writer, data uint64) os.Error {
	return w.PrintfLine(":%d", data)
}

func encodeBytes(w *textproto.Writer, data []byte) (err os.Error) {
	if err = w.PrintfLine("$%d", len(data)); err != nil {
		return
	}
	if err = w.PrintfLine("%s", data); err != nil {
		return
	}
	return nil
}

func encodeSlice(w *textproto.Writer, data []interface{}) (err os.Error) {
	if err = w.PrintfLine("*%d", len(data)); err != nil {
		return
	}
	for _, part := range data {
		if err = encode(w, part); err != nil {
			return
		}
	}
	return nil
}
