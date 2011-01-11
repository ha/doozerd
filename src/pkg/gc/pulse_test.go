package gc

import (
	"github.com/bmizerany/assert"
	"os"
	"testing"
)

// Testing

type FakeSetter struct {
	path chan string
	body chan []byte
}

func (fs *FakeSetter) Set(path, oldCas string, body []byte) (string, os.Error) {
	fs.path <- path
	fs.body <- body
	return "123", nil
}

func TestGcPulse(t *testing.T) {
	seqns := make(chan int64)
	defer close(seqns)
	fs := &FakeSetter{make(chan string), make(chan []byte)}

	go Pulse("test", seqns, fs, 1)

	seqns <- 0
	assert.Equal(t, "/doozer/info/test/applied", <-fs.path)
	assert.Equal(t, []byte{'0'}, <-fs.body)

	seqns <- 1
	assert.Equal(t, "/doozer/info/test/applied", <-fs.path)
	assert.Equal(t, []byte{'1'}, <-fs.body)
}
