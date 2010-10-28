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

func TestProtoEncodeInt(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, ":0\r\n", string(b.Bytes()))
}

func TestProtoEncodeBytes(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, []byte{'a'})
	assert.Equal(t, nil, err)
	assert.Equal(t, "$1\r\na\r\n", string(b.Bytes()))
}

func TestProtoEncodeString(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, Line("hi"))
	assert.Equal(t, nil, err)
	assert.Equal(t, "+hi\r\n", string(b.Bytes()))
}

func TestProtoEncodeError(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, os.NewError("hi"))
	assert.Equal(t, nil, err)
	assert.Equal(t, "-hi\r\n", string(b.Bytes()))
}

func TestProtoEncodeNil(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, "$-1\r\n", string(b.Bytes()))
}

func TestProtoEncodeMulti(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, []interface{}{[]byte{'a'}, []byte{'b'}})
	assert.Equal(t, nil, err)
	assert.Equal(t, "*2\r\n$1\r\na\r\n$1\r\nb\r\n", string(b.Bytes()))
}

func TestProtoEncodeStrings(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, []interface{}{"GET", "FOO"})
	assert.Equal(t, nil, err)
	assert.Equal(t, "*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n", string(b.Bytes()), "")
}

func TestProtoEncodeDeeper(t *testing.T) {
	b := new(bytes.Buffer)
	w := bufio.NewWriter(b)
	ww := textproto.NewWriter(w)

	err := encode(ww, []interface{}{[]byte{'a'}, 1, []interface{}{"a", "b"}})
	assert.Equal(t, nil, err)
	assert.Equal(t, "*3\r\n$1\r\na\r\n:1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n", string(b.Bytes()))
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
