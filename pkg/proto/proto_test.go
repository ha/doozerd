package proto

import (
	"bufio"
	"bytes"
	"io"
	"junta/assert"
	"junta/test"
	"os"
	"reflect"
	"testing"
	"testing/quick"
)

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
	encoding string
	data     interface{}
}

var encTests = []encTest{
	{":0\r\n", int(0)},
	{":0\r\n", int8(0)},
	{":0\r\n", int16(0)},
	{":0\r\n", int32(0)},
	{":0\r\n", int64(0)},
	{":0\r\n", uint(0)},
	{":0\r\n", uint8(0)}, // aka byte
	{":0\r\n", uint16(0)},
	{":0\r\n", uint32(0)},
	{":0\r\n", uint64(0)},

	{"$3\r\nfoo\r\n", []byte{'f', 'o', 'o'}},
	{"$3\r\nfoo\r\n", "foo"},
	{"+hi\r\n", Line("hi")},
	{"-hi\r\n", os.NewError("hi")},
	{"$-1\r\n", nil},
	{"*2\r\n$1\r\na\r\n$1\r\nb\r\n", []interface{}{[]byte{'a'}, []byte{'b'}}},
	{"*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n", []string{"GET", "FOO"}},
	{"*2\r\n:1\r\n*1\r\n:1\r\n", []interface{}{1, []interface{}{1}}},
}

var decTests = []encTest{
	{":0\r\n", int64(0)},
	{":1\r\n", int64(1)},
	{":-1\r\n", int64(-1)},
	{":18446744073709551611\r\n", uint64(18446744073709551611)},
	{"$3\r\nfoo\r\n", []byte{'f', 'o', 'o'}},
	{"+hi\r\n", []byte("hi")},
	{"-hi\r\n", ResponseError("hi")},
	{"$-1\r\n", nil},
	{"*2\r\n$1\r\na\r\n$1\r\nb\r\n", []interface{}{[]byte{'a'}, []byte{'b'}}},
	{"*1\r\n*1\r\n:1\r\n", []interface{}{[]interface{}{int64(1)}}},
	{"\r\n:0\r\n", int64(0)}, // ignore blank lines
}

var decErrTests = []string{
	":111118446744073709551611\r\n", // out of range for uint64
	"$18446744073709551611\r\n", // out of range for int
	"*18446744073709551611\r\n", // out of range for int
}

func TestProtoEncodeVal(t *testing.T) {
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

func TestProtoEncodeShort(t *testing.T) {
	for _, e := range encTests {
		n := len(e.encoding)
		for i := 0; i < n; i++ {
			t.Logf("trying %d bytes", i)
			w := test.ErrWriter{i}
			err := encode(&w, e.data)
			if err == nil {
				t.Errorf("expected an error encoding %T %v in %d bytes", e.data, e.data, i)
			}
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

func TestProtoDecodeVal(t *testing.T) {
	for _, e := range decTests {
		b := bytes.NewBufferString(e.encoding)
		r := bufio.NewReader(b)
		data, err := decode(r)
		if err != nil {
			t.Errorf("in %q,", e.encoding)
			t.Error("unexpected err:", err)
			continue
		}
		if !reflect.DeepEqual(e.data, data) {
			t.Errorf("in %q,", e.encoding)
			t.Errorf("expected %T, %v", e.data, e.data)
			t.Errorf("     got %T, %v", data, data)
		}
	}
}

func TestProtoDecodeShort(t *testing.T) {
	for _, e := range decTests {
		n := len(e.encoding)
		for i := 0; i < n; i++ {
			s := e.encoding[0:i]
			r := bufio.NewReader(bytes.NewBufferString(s))
			_, err := decode(r)
			if err == nil {
				t.Errorf("expected an error from %q", s)
			}
		}
	}
}

func TestProtoDecodeErr(t *testing.T) {
	for _, s := range decErrTests {
		r := bufio.NewReader(bytes.NewBufferString(s))
		_, err := decode(r)
		if err == nil {
			t.Errorf("expected an error from %q", s)
		}
	}
}

func decodeFuzz(s string) (b bool) {
	defer func() {
		v := recover()
		if v != nil {
			b = false
		}
	}()
	decode(bufio.NewReader(bytes.NewBufferString(s)))
	return true
}

func TestDecodeFuzz(t *testing.T) {
	if err := quick.Check(decodeFuzz, nil); err != nil {
		t.Error(err)
	}
}
