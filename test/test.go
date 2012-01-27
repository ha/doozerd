package test

import (
	"github.com/ha/doozerd/store"
	"io"
	"sync/atomic"
)

type FakeProposer struct {
	*store.Store
	seqn int64
}

func (fp *FakeProposer) Propose(v []byte) store.Event {
	n := atomic.AddInt64(&fp.seqn, 1)

	ch, err := fp.Wait(store.Any, n)
	if err != nil {
		panic(err)
	}
	fp.Ops <- store.Op{n, string(v), false}
	return <-ch
}

// An io.Writer that will return os.EOF on the `n`th byte written
type ErrWriter struct {
	N int
}

func (e *ErrWriter) Write(p []byte) (n int, err error) {
	l := len(p)
	e.N -= l
	if e.N <= 0 {
		return 0, io.EOF
	}
	return l, nil
}
