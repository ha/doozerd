package test

import (
	"junta/store"
	"os"
)

type FakeProposer struct {
	*store.Store
	seqn uint64
}

func (fp *FakeProposer) Propose(v string) (uint64, os.Error) {
	fp.seqn++
	ch := fp.Wait(fp.seqn)
	fp.Apply(fp.seqn, v)
	ev := <-ch
	return fp.seqn, ev.Err
}
