package paxos

import (
	"doozer/assert"
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

func TestChanPutCloser(t *testing.T) {
	c := make(ChanPutCloser)
	msg := newInvite(0) // any old Msg will do here
	c.Put(msg)
	assert.Equal(t, msg, <-c, "")
}
