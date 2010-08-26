package paxos

import (
    "borg/assert"
    "log"
    "testing"
)

func selfRefNewManager(start uint64, self string, nodes []string, logger *log.Logger) *Manager {
	p := make([]Putter, 1)
	m := NewManager(start, self, nodes, logger)
	m.Init(FakePutter(p))
	p[0] = m
	return m
}

func TestProposeAndLearn(t *testing.T) {
	exp := "foo"
	m := selfRefNewManager(1, "a", []string{"a"}, logger)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")
}

func TestProposeAndRecv(t *testing.T) {
	exp := "foo"
	m := selfRefNewManager(1, "a", []string{"a"}, logger)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvAltStart(t *testing.T) {
	exp := "foo"
	m := selfRefNewManager(2, "a", []string{"a"}, logger)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvMultiple(t *testing.T) {
	exp := []string{"foo", "bar"}
	seqnexp := []uint64{1, 2}
	m := selfRefNewManager(1, "a", []string{"a"}, logger)

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
	m := selfRefNewManager(1, "a", []string{"a"}, logger)

	m.Put(newVoteFrom(1, 1, exp))
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestNewInstanceBecauseOfMessageTriangulate(t *testing.T) {
	exp := "bar"
	m := selfRefNewManager(1, "a", []string{"a"}, logger)

	m.Put(newVoteFrom(1, 1, exp))
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}
