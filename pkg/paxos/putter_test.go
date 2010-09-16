package paxos

import (
	"junta/assert"
	"testing"
)

func TestPutToWrapper(t *testing.T) {
    seqn := uint64(1)
    p := make(ChanPutCloserTo)
    w := putToWrapper{seqn, p}
    m := newInvite(1)
    a := "a"

    w.PutTo(m, a)

    exp := m.Dup()
    exp.SetSeqn(seqn)
    assert.Equal(t, Packet{exp, a}, <-p)
}

