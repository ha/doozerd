package paxos

import (
    "borg/assert"
    "testing"
)

func TestProposeAndLearn(t *testing.T) {
	exp := "foo"
	m := NewManager(1, 1, 1, logger)
	m.Init(m)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")
}

func TestProposeAndRecv(t *testing.T) {
	exp := "foo"
	m := NewManager(1, 1, 1, logger)
	m.Init(m)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvAltStart(t *testing.T) {
	exp := "foo"
	m := NewManager(1, 2, 1, logger)
	m.Init(m)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvMultiple(t *testing.T) {
	exp := []string{"foo", "bar"}
	seqnexp := []uint64{1, 2}
	m := NewManager(1, 1, 1, logger)
	m.Init(m)

	got0 := m.Propose(exp[0])
	assert.Equal(t, exp[0], got0, "")

	got1 := m.Propose(exp[1])
	assert.Equal(t, exp[1], got1, "")

	seqn0, v0 := m.Recv()
	assert.Equal(t, seqnexp[0], seqn0, "seqn 1")
	assert.Equal(t, exp[0], v0, "")

	seqn1, v1 := m.Recv()
	assert.Equal(t, seqnexp[1], seqn1, "seqn 2")
	assert.Equal(t, exp[1], v1, "")
}

func TestNewInstanceBecauseOfMessage(t *testing.T) {
	exp := "foo"
	m := NewManager(1, 1, 1, logger)
	m.Init(m)

	m.Put(Msg{1, 1, 1, "VOTE", "1:" + exp})
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestNewInstanceBecauseOfMessageTriangulate(t *testing.T) {
	exp := "bar"
	m := NewManager(1, 1, 1, logger)
	m.Init(m)

	m.Put(Msg{1, 1, 1, "VOTE", "1:" + exp})
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}
