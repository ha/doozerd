package proto

import (
	"bytes"
	"bufio"
	"os"
	"io"

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

func TestProtoEncodef(t *testing.T) {
	buf := new(bytes.Buffer)
	Encodef(buf, "GET", "FOO")
	assert.Equal(t, "*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n", string(buf.Bytes()), "")
}

func TestProtoEncodefHeaderError(t *testing.T) {
	buf := new(bytes.Buffer)
	ew := &ErroneousWriter{buf, 0}
	err := Encodef(ew, "GET", "FOO")
	assert.T(t, err != nil)
}

func TestProtoEncodefBodyError(t *testing.T) {
	buf := new(bytes.Buffer)
	ew := &ErroneousWriter{buf, 1}
	err := Encodef(ew, "GET", "FOO")
	assert.T(t, err != nil)
}

func TestProtoDecodeEmptyLine(t *testing.T) {
	b := new(bytes.Buffer)
	r := bufio.NewReader(b)

	_, err := Decode(r)
	assert.T(t, err != nil)
}

func TestProtoDecodeNonEmpty(t *testing.T) {
	b := new(bytes.Buffer)
	br := bufio.NewReader(b)

	Encodef(b, "SET", "foo", "bar")
	parts, err := Decode(br)

	assert.Equal(t, nil, err, "")
	assert.Equal(t, []string{"SET", "foo", "bar"}, parts, "")

	parts, err = Decode(br)
	assert.Equal(t, os.EOF, err, "")
	assert.Equal(t, []string{}, parts, "")
}
