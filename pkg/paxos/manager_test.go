package paxos

import (
    "borg/assert"
    "testing"
)

type Result struct {
	seqn uint64
	v string
}

type instReq struct {
	seqn uint64
	ch chan *Instance
}

type Manager struct{
	learned chan Result
	reqs chan instReq
}

func NewManager() *Manager {
	m := &Manager{
		learned: make(chan Result),
		reqs: make(chan instReq),
	}
	return m
}

func (m *Manager) Init(outs Putter) {
	go func() {
		instances := make(map[uint64]*Instance)
		for req := range m.reqs {
			inst, ok := instances[req.seqn]
			if !ok {
				inst = NewInstance(1)
				inst.Init(PutWrapper{1, outs})
				instances[req.seqn] = inst
				go func() {
					m.learned <- Result{req.seqn, inst.Value()}
				}()
			}
			req.ch <- inst
		}
	}()
}

func (m *Manager) getInstance(seqn uint64) *Instance {
	ch := make(chan *Instance)
	m.reqs <- instReq{seqn, ch}
	return <-ch
}

func (m *Manager) Put(msg Msg) {
	m.getInstance(msg.seqn).Put(msg)
}

func (m *Manager) Propose(v string) string {
	inst := m.getInstance(1)
	inst.Propose(v)
	return inst.Value()
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	return result.seqn, result.v
}

// Testing

func TestProposeAndLearn(t *testing.T) {
	exp := "foo"
	m := NewManager()
	m.Init(m)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")
}

func TestProposeAndRecv(t *testing.T) {
	exp := "foo"
	m := NewManager()
	m.Init(m)

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestNewInstanceBecauseOfMessage(t *testing.T) {
	exp := "foo"
	m := NewManager()
	m.Init(m)

	m.Put(Msg{1, 1, 1, "VOTE", "1:" + exp})
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestNewInstanceBecauseOfMessageTriangulate(t *testing.T) {
	exp := "bar"
	m := NewManager()
	m.Init(m)

	m.Put(Msg{1, 1, 1, "VOTE", "1:" + exp})
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}
