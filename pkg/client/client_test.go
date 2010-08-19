package client

import (
	"bytes"
	"os"
	"io"
	"fmt"
	"bufio"
	"strings"
	"strconv"

	"testing"
	//"testing/iotest"
	"borg/assert"
)

type ReadStringer interface {
	io.Reader
	ReadString(byte) (string, os.Error)
}

func Decode(r ReadStringer) (parts []string, err os.Error) {
	var count int = 1
	var size int
	var line string

Loop:
	for count > 0 {
		// TODO: test if len(line) == 0
		line, err = r.ReadString('\n')
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
			n, err := io.ReadFull(r, buf)
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

func Encode(w io.Writer, parts ... string) (err os.Error) {
	_, err = fmt.Fprintf(w, "*%d\r\n", len(parts))
	for _, part := range parts {
		_, err = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(part), part)
	}
	return err
}


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

func TestProtoEncode(t *testing.T) {
	buf := new(bytes.Buffer)
	Encode(buf, "GET", "FOO")
	assert.Equal(t, "*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n", string(buf.Bytes()), "")
}

func TestProtoEncodeHeaderError(t *testing.T) {
	buf := new(bytes.Buffer)
	ew := &ErroneousWriter{buf, 0}
	err := Encode(ew, "GET", "FOO")
	assert.True(t, err != nil)
}

func TestProtoEncodeBodyError(t *testing.T) {
	buf := new(bytes.Buffer)
	ew := &ErroneousWriter{buf, 1}
	err := Encode(ew, "GET", "FOO")
	assert.True(t, err != nil)
}

func TestProtoDecodeEmptyLine(t *testing.T) {
	b := new(bytes.Buffer)
	r := bufio.NewReader(b)

	_, err := Decode(r)
	assert.True(t, err != nil)
}

func TestProtoDecodeNonEmpty(t *testing.T) {
	b := new(bytes.Buffer)
	br := bufio.NewReader(b)

	Encode(b, "SET", "foo", "bar")
	parts, _ := Decode(br)

	assert.Equal(t, []string{"SET", "foo", "bar"}, parts, "")
}
