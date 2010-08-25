package paxos

import (
    "borg/assert"
    "testing"
)

func TestCluster(t *testing.T) {
    outs := make(chan Message)
    cx := NewCluster("c", []string{"a", "b", "c"}, ChanPutter(outs))
    assert.Equal(t, 3, cx.Len(), "Len")
    assert.Equal(t, 2, cx.Quorum(), "Quorum")
    assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
    msg := m("1:1:INVITE:1")
    cx.Put(msg)
    assert.Equal(t, msg, <-outs, "Put")
}
