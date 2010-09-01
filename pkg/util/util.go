package util

import (
	"fmt"
	"io"
	"log"
	"os"
)

var urandom = MustOpen("/dev/urandom", os.O_RDONLY, 0)

var NullLogger = log.New(nullWriter{}, nil, "", 0)

type nullWriter struct{}

func (nw nullWriter) Write(p []byte) (int, os.Error) {
	return len(p), nil
}

// MustOpen is like os.Open but panics if the file cannot be opened. It
// simplifies safe initialization of global variables holding file descriptors.
func MustOpen(name string, flag int, perm uint32) *os.File {
	file, err := os.Open(name, flag, perm)
	if err != nil {
		panic(err)
	}
	return file
}

func RandBytes(b []byte) {
	n, err := io.ReadFull(urandom, b)
	if err != nil {
		panic(err)
	}
	if n != len(b) {
		panic(fmt.Sprintf("RandBytes: %d != %d", n, len(b)))
	}
}

func Packui64(b []byte, n uint64) {
	l := len(b)
	for i := range b {
		b[l-i-1] = uint8(n & 0xff)
		n >>= 8
	}
}

func Unpackui64(b []byte) (n uint64) {
	for _, k := range b {
		n <<= 8
		n |= uint64(k)
	}
	return
}

func NewLogger(format string, a ... interface{}) *log.Logger {
	prefix := fmt.Sprintf(format, a)

	if prefix == "" {
		panic("always give a prefix!")
	}

	return log.New(
		os.Stderr, nil,
		"juntad: " + prefix + " ",
		log.Lok | log.Lshortfile,
	)
}

func RandString(bits int) string {
	buf := make([]byte, bits/8)
	RandBytes(buf)
	return fmt.Sprintf("%x", buf)
}
