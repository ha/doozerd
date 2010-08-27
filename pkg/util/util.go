package util

import (
	"fmt"
	"io"
	"os"
)

var urandom = MustOpen("/dev/urandom", os.O_RDONLY, 0)

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
