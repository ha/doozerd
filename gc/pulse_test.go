package gc

import (
	"github.com/bmizerany/assert"
	"github.com/ha/doozerd/store"
	"testing"
)

// Testing

type FakeProposer chan string

func (fs FakeProposer) Propose(v []byte) (e store.Event) {
	fs <- string(v)
	e.Rev = 123
	return
}

func TestGcPulse(t *testing.T) {
	seqns := make(chan int64)
	defer close(seqns)
	fs := make(FakeProposer)

	go Pulse("test", seqns, fs, 1)

	seqns <- 0
	assert.Equal(t, "-1:/ctl/node/test/applied=0", <-fs)

	seqns <- 1
	assert.Equal(t, "-1:/ctl/node/test/applied=1", <-fs)
}
