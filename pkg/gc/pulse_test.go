package gc

import (
	"doozer/assert"
	"os"
	"testing"
)

// Testing

type FakeSetter struct {
	path, body chan string
}

func (fs *FakeSetter) Set(path, body, oldCas string) (string, os.Error) {
	fs.path <- path
	fs.body <- body
	return "123", nil
}

func TestGcPulse(t *testing.T) {
	seqns := make(chan uint64)
	defer close(seqns)
	fs := &FakeSetter{make(chan string), make(chan string)}

	go Pulse("test", seqns, fs, 1)

	seqns <- 0
	assert.Equal(t, "/doozer/info/test/applied", <-fs.path)
	assert.Equal(t, "0", <-fs.body)

	seqns <- 1
	assert.Equal(t, "/doozer/info/test/applied", <-fs.path)
	assert.Equal(t, "1", <-fs.body)
}
