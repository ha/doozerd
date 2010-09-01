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

func TestProtoencode(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	encode(ww, "GET", "FOO")
	assert.Equal(t, "*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n", string(b.Bytes()), "")
}

func TestProtoencodeHeaderError(t *testing.T) {
	b := new(bytes.Buffer)
	ew := &ErroneousWriter{b, 0}
	w := bufio.NewWriter(ew)
	ww := textproto.NewWriter(w)

	err := encode(ww, "GET", "FOO")
	assert.T(t, err != nil)
}

func TestProtoencodeBodyError(t *testing.T) {
	b := new(bytes.Buffer)
	ew := &ErroneousWriter{b, 1}
	w := bufio.NewWriter(ew)
	ww := textproto.NewWriter(w)

	err := encode(ww, "GET", "FOO")
	assert.T(t, err != nil)
}

func TestProtoDecodeEmptyLine(t *testing.T) {
	b := new(bytes.Buffer)
	r := bufio.NewReader(b)
	rr := textproto.NewReader(r)

	_, err := decode(rr)
	assert.T(t, err != nil)
}

func TestProtoDecodeNonEmpty(t *testing.T) {
	b := new(bytes.Buffer)
	r := bufio.NewReader(b)
	rr := textproto.NewReader(r)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	encode(ww, "SET", "foo", "bar")
	parts, err := decode(rr)

	assert.Equal(t, nil, err, "")
	assert.Equal(t, []string{"SET", "foo", "bar"}, parts, "")

	parts, err = decode(rr)
	assert.Equal(t, os.EOF, err, "")
	assert.Equal(t, []string{}, parts, "")
}
