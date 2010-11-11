package proto

import (
	"bufio"
	"junta/util"
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

var crnl = []byte{'\r', '\n'}

var logger = util.NewLogger("proto")

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
	err := encode(c.W, data)
	if err != nil {
		return &ProtoError{id, SendRes, err}
	}
	return c.W.Flush()
}

func (c *Conn) SendError(id uint, msg string) os.Error {
	return c.SendResponse(id, os.NewError("ERR: "+msg))
}

func (c *Conn) SendRedirect(id uint, addr string) os.Error {
	return c.SendResponse(id, os.NewError("REDIRECT: "+addr))
}

func (c *Conn) ReadRequest() (uint, []string, os.Error) {
	id := c.Next()
	c.StartRequest(id)
	data, err := decode(c.R)
	c.EndRequest(id)
	if err != nil {
		if err == os.EOF {
			return 0, nil, err
		} else {
			return 0, nil, &ProtoError{id, ReadReq, err}
		}
	}
	logger.Println("got data", data)
	parts, ok := stringParts(data)
	if !ok {
		return 0, nil, &ProtoError{id, ReadReq, os.NewError("not strings")}
	}
	return id, parts, nil
}

// Client functions

func (c *Conn) SendRequest(data interface{}) (uint, os.Error) {
	id := c.Next()
	c.StartRequest(id)
	defer c.EndRequest(id)
	err := encode(c.W, data)
	if err != nil {
		return 0, &ProtoError{id, SendReq, err}
	}
	return id, c.W.Flush()
}

func (c *Conn) ReadResponse(id uint) ([]string, os.Error) {
	c.StartResponse(id)
	defer c.EndResponse(id)

	data, err := decode(c.R)

	switch terr := err.(type) {
	default:
		return nil, &ProtoError{id, ReadRes, err}
	case nil:
		logger.Println("got data", data)
		parts, ok := stringParts(data)
		if !ok {
			return nil, &ProtoError{id, ReadReq, os.NewError("not strings")}
		}
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

func decode(r *bufio.Reader) (data interface{}, err os.Error) {
	var line string

	for len(line) < 1 {
		line, err = readLine(r)
		if err != nil {
			return nil, err
		}
	}

	switch line[0] {
	case ':':
		i, e := strconv.Atoi64(line[1:])
		if e != nil {
			if ne, ok := e.(*strconv.NumError); ok && ne.Error == os.ERANGE {
				return strconv.Atoui64(line[1:])
			}
			return nil, e
		}
		return i, nil
	case '+':
		return []byte(line[1:]), nil
	case '-':
		return ResponseError(line[1:]), nil
	case '$':
		n, e := strconv.Atoi(line[1:])
		if e != nil {
			return nil, e
		}

		// in redis, "$-1" means nil
		if n < 0 {
			return nil, nil
		}

		b := make([]byte, n+2) // add space for trailing CR+LF
		_, e = io.ReadFull(r, b)
		if e != nil {
			return nil, e
		}
		if b[n] != '\r' || b[n+1] != '\n' {
			// TODO use ProtoError
			return nil, os.NewError("expected trailing CR+LF")
		}
		return b[0:n], nil
	case '*':
		n, e := strconv.Atoi(line[1:])
		if e != nil {
			return nil, e
		}

		d := make([]interface{}, n)
		for i := range d {
			d[i], err = decode(r)
			if err != nil {
				return nil, err
			}
		}
		return d, nil
	}

	return nil, os.NewError("unknown line: " + line) // TODO use ProtoError
}

func encode(w io.Writer, data interface{}) (err os.Error) {
	switch t := data.(type) {
	default:
		return os.NewError(fmt.Sprintf("unexpected type %T", t))
	case Line:
		if err = printfLine(w, "+%s", t); err != nil {
			return
		}
	case os.Error:
		if err = printfLine(w, "-%s", t.String()); err != nil {
			return
		}
	case nil:
		if err = printfLine(w, "$-1"); err != nil {
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

func encodeInt64(w io.Writer, data int64) os.Error {
	return printfLine(w, ":%d", data)
}

func encodeUint64(w io.Writer, data uint64) os.Error {
	return printfLine(w, ":%d", data)
}

func encodeBytes(w io.Writer, data []byte) (err os.Error) {
	if err = printfLine(w, "$%d", len(data)); err != nil {
		return
	}
	if err = printfLine(w, "%s", data); err != nil {
		return
	}
	return nil
}

func encodeSlice(w io.Writer, data []interface{}) (err os.Error) {
	if err = printfLine(w, "*%d", len(data)); err != nil {
		return
	}
	for _, part := range data {
		if err = encode(w, part); err != nil {
			return
		}
	}
	return nil
}

func printfLine(w io.Writer, format string, args ...interface{}) os.Error {
	_, err := fmt.Fprintf(w, format, args...)
	if err != nil {
		return err
	}
	_, err = w.Write(crnl)
	return err
}

func readLine(r *bufio.Reader) (string, os.Error) {
	line, err := readLineBytes(r)
	return string(line), err
}

func readLineBytes(r *bufio.Reader) ([]byte, os.Error) {
	line, err := r.ReadBytes('\n')
	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		n--
		if n > 0 && line[n-1] == '\r' {
			n--
		}
	}
	return line[0:n], err
}

func stringParts(d interface{}) (a []string, ok bool) {
	t, ok := d.([]interface{})
	if !ok {
		return nil, false
	}
	a = make([]string, len(t))
	for i, x := range t {
		b, ok := x.([]byte)
		if !ok {
			return nil, false
		}
		a[i] = string(b)
	}
	return a, true
}
