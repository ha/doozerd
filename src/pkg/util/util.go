package util

import (
	"fmt"
	"io"
	"os"
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

func Packi64(b []byte, n int64) {
	l := len(b)
	for i := range b {
		b[l-i-1] = uint8(n & 0xff)
		n >>= 8
	}
}

func Unpacki64(b []byte) (n int64) {
	for _, k := range b {
		n <<= 8
		n |= int64(k)
	}
	return
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
