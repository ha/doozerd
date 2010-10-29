package proto

import (
	"bytes"
	"bufio"
	"os"
	"io"
	"net/textproto"

	"testing"
	"junta/assert"
)


// == Testing =============================

type ErroneousWriter struct {
	// No exported fields
	io.Writer
	which int
}

func (ew *ErroneousWriter) Write(bytes []byte) (int, os.Error) {
	if ew.which == 0 {
		return 0, os.NewError("BOOM!")
	}
	ew.which--
	return ew.Writer.Write(bytes)
}

type encTest struct {
	data     interface{}
	encoding string
}

var encTests = []encTest{
	{int(0), ":0\r\n"},
	{int8(0), ":0\r\n"},
	{int16(0), ":0\r\n"},
	{int32(0), ":0\r\n"},
	{int64(0), ":0\r\n"},
	{uint(0), ":0\r\n"},
	{uint8(0), ":0\r\n"}, // aka byte
	{uint16(0), ":0\r\n"},
	{uint32(0), ":0\r\n"},
	{uint64(0), ":0\r\n"},

	{[]byte{'a'}, "$1\r\na\r\n"},
	{Line("hi"), "+hi\r\n"},
	{os.NewError("hi"), "-hi\r\n"},
	{nil, "$-1\r\n"},
	{[]interface{}{[]byte{'a'}, []byte{'b'}}, "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
	{[]string{"GET", "FOO"}, "*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n"},
	{[]interface{}{1, []interface{}{1}}, "*2\r\n:1\r\n*1\r\n:1\r\n"},
}

func TestProtoEncode(t *testing.T) {
	for _, e := range encTests {
		b := new(bytes.Buffer)
		w := bufio.NewWriter(b)
		ww := textproto.NewWriter(w)

		err := encode(ww, e.data)
		if err != nil {
			t.Error("unexpected err:", err)
			continue
		}
		if e.encoding != string(b.Bytes()) {
			t.Error("expected %q", e.encoding)
			t.Error("     got %q", string(b.Bytes()))
		}
	}
}

func TestProtoEncodeHeaderError(t *testing.T) {
	b := new(bytes.Buffer)
	ew := &ErroneousWriter{b, 0}
	w := bufio.NewWriter(ew)
	ww := textproto.NewWriter(w)

	err := encode(ww, []string{"GET", "FOO"})
	assert.T(t, err != nil)
}

func TestProtoEncodeBodyError(t *testing.T) {
	b := new(bytes.Buffer)
	ew := &ErroneousWriter{b, 1}
	w := bufio.NewWriter(ew)
	ww := textproto.NewWriter(w)

	err := encode(ww, []string{"GET", "FOO"})
	assert.T(t, err != nil)
}

func TestProtoDecodeEmptyLine(t *testing.T) {
	b := new(bytes.Buffer)
	r := bufio.NewReader(b)
	rr := textproto.NewReader(r)

	_, err := decode(rr)
	assert.T(t, err != nil)
}

func TestProtoDecodeRecievedError(t *testing.T) {
	b := bytes.NewBufferString("-ERR: foo\r\n")
	r := bufio.NewReader(b)
	rr := textproto.NewReader(r)

	_, err := decode(rr)
	assert.Equal(t, "ERR: foo", err.String())
}

func TestProtoDecodeNonEmpty(t *testing.T) {
	b := new(bytes.Buffer)
	r := bufio.NewReader(b)
	rr := textproto.NewReader(r)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	encode(ww, []interface{}{"SET", "foo", "bar"})
	parts, err := decode(rr)

	assert.Equal(t, nil, err, "")
	assert.Equal(t, []string{"SET", "foo", "bar"}, parts, "")

	parts, err = decode(rr)
	assert.Equal(t, os.EOF, err, "")
	assert.Equal(t, []string{}, parts, "")
}
