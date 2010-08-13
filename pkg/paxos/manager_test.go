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
	seqns chan uint64
}

func NewManager() *Manager {
	m := &Manager{
		learned: make(chan Result),
		reqs: make(chan instReq),
		seqns: make(chan uint64),
	}
	return m
}

func (m *Manager) Init(outs Putter) {
	go func() {
		instances := make(map[uint64]*Instance)
		for req := range m.reqs {
			inst, ok := instances[req.seqn]
			if !ok {
				inst = NewInstance(1, 1)
				inst.Init(PutWrapper{req.seqn, outs})
				instances[req.seqn] = inst
				go func() {
					m.learned <- Result{req.seqn, inst.Value()}
				}()
			}
			req.ch <- inst
		}
	}()

	// Generate an infinite stream of sequence numbers (seqns).
	go func() {
		for n := uint64(1); ; n++ {
			m.seqns <- n
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
	inst := m.getInstance(<-m.seqns)
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

func TestProposeAndRecvMultiple(t *testing.T) {
	exp := []string{"foo", "bar"}
	seqnexp := []uint64{1, 2}
	m := NewManager()
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
