package paxos

import (
	"github.com/bmizerany/assert"
	"testing"
)

type fakePutterTo struct {
	addrs []string
	msgs  []*M
}


func (f *fakePutterTo) PutTo(m *M, addr string) {
	f.addrs = append(f.addrs, addr)
	f.msgs = append(f.msgs, m)
}


func TestPutToWrapper(t *testing.T) {
	seqn := int64(1)
	var p fakePutterTo
	w := putToWrapper{seqn, &p}
	m := newInvite(1)
	a := "a"

	w.PutTo(m, a)

	exp := m.Dup()
	exp.SetSeqn(seqn)
	assert.Equal(t, 1, len(p.addrs))
	assert.Equal(t, a, p.addrs[0])
	assert.Equal(t, exp, p.msgs[0])
}
