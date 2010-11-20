package gc

import (
	"doozer/assert"
	"doozer/store"
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
	st := store.New()
	fs := &FakeSetter{make(chan string), make(chan string)}

	go Pulse("test", st, fs, 1)

	assert.Equal(t, "/doozer/info/test/applied", <-fs.path)
	assert.Equal(t, "0", <-fs.body)

	st.Ops <- store.Op{1, store.Nop}
	assert.Equal(t, "/doozer/info/test/applied", <-fs.path)
	assert.Equal(t, "1", <-fs.body)
}
