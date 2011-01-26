package gc

import (
	"github.com/bmizerany/assert"
	"os"
	"testing"
)

// Testing

type FakeProposer chan string

func (fs FakeProposer) Propose(v string, _ chan bool) (int64, int64, os.Error) {
	fs <- v
	return 123, 123, nil
}

func TestGcPulse(t *testing.T) {
	seqns := make(chan int64)
	defer close(seqns)
	fs := make(FakeProposer)

	go Pulse("test", seqns, fs, 1)

	seqns <- 0
	assert.Equal(t, "0:/doozer/info/test/applied=0", <-fs)

	seqns <- 1
	assert.Equal(t, "123:/doozer/info/test/applied=1", <-fs)
}
