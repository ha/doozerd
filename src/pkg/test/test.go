package test

import (
	"doozer/store"
	"os"
)

type FakeProposer struct {
	*store.Store
	seqn int64
}

func (fp *FakeProposer) Propose(v string, cancel chan bool) (int64, string, os.Error) {
	fp.seqn++
	ch := fp.Wait(fp.seqn)
	fp.Ops <- store.Op{fp.seqn, v}
	ev := <-ch
	return fp.seqn, ev.Cas, ev.Err
}

// An io.Writer that will return os.EOF on the `n`th byte written
type ErrWriter struct {
	N int
}

func (e *ErrWriter) Write(p []byte) (n int, err os.Error) {
	l := len(p)
	e.N -= l
	if e.N <= 0 {
		return 0, os.EOF
	}
	return l, nil
}
