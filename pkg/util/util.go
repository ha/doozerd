package util

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// Sufficient for 10**6 simultaneous IDs with probability of collision less
// than 10**-7.
const IdBits = 64

type NullWriter struct{}

func (nw NullWriter) Write(p []byte) (int, os.Error) {
	return len(p), nil
}

// Misc
var (
	urandom = MustOpen("/dev/urandom", os.O_RDONLY, 0)
)

// Logging
var (
	LogWriter io.Writer = NullWriter{}
)

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

	if strings.HasPrefix(prefix, "udp") ||
		strings.HasPrefix(prefix, "manager") ||
		strings.HasPrefix(prefix, "store") ||
		strings.HasPrefix(prefix, "ws://") ||
		strings.HasPrefix(prefix, "127.0.0.1:") ||
		strings.HasPrefix(prefix, "acceptor") {
		return log.New(NullWriter{}, nil, "", log.Lok | log.Lshortfile)
	}

	return log.New(
		LogWriter, nil,
		"juntad: " + prefix + " ",
		log.Lok | log.Lshortfile,
	)
}

func RandHexString(bits int) string {
	buf := make([]byte, bits/8)
	RandBytes(buf)
	return fmt.Sprintf("%x", buf)
}

// Generates a random string with IdBits bits of entropy. The string will
// contain only the characters [0-9a-f] and dot, formatted for easier reading.
func RandId() string {
	return RandHexString(IdBits/2) + "." +
	       RandHexString(IdBits/2)
}
