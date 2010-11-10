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

	{[]byte{'f', 'o', 'o'}, "$3\r\nfoo\r\n"},
	{"foo", "$3\r\nfoo\r\n"},
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
		err := encode(b, e.data)
		if err != nil {
			t.Error("unexpected err:", err)
			continue
		}
		if e.encoding != string(b.Bytes()) {
			t.Errorf("expected %q", e.encoding)
			t.Errorf("     got %q", b.String())
		}
	}
}

func TestProtoEncodeHeaderError(t *testing.T) {
	b := new(bytes.Buffer)
	ew := &ErroneousWriter{b, 0}

	err := encode(ew, []string{"GET", "FOO"})
	assert.T(t, err != nil, err)
}

func TestProtoEncodeBodyError(t *testing.T) {
	b := new(bytes.Buffer)
	ew := &ErroneousWriter{b, 1}

	err := encode(ew, []string{"GET", "FOO"})
	assert.T(t, err != nil, err)
}

func TestProtoDecodeEmptyLine(t *testing.T) {
	b := new(bytes.Buffer)
	_, err := decode(bufio.NewReader(b))
	assert.T(t, err != nil, err)
}

func TestProtoDecodeRecievedError(t *testing.T) {
	b := bytes.NewBufferString("-ERR: foo\r\n")
	_, err := decode(bufio.NewReader(b))
	assert.Equal(t, "ERR: foo", err.String())
}

func TestProtoDecodeNonEmpty(t *testing.T) {
	b := new(bytes.Buffer)
	r := bufio.NewReader(b)

	encode(b, []interface{}{"SET", "foo", "bar"})
	parts, err := decode(r)

	assert.Equal(t, nil, err, "")
	assert.Equal(t, []string{"SET", "foo", "bar"}, parts, "")

	parts, err = decode(r)
	assert.Equal(t, os.EOF, err, "")
	assert.Equal(t, []string{}, parts, "")
}
