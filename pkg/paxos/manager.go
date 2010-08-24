package paxos

import (
	"log"
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
	me uint64
	learned chan Result
	reqs chan instReq
	seqns chan uint64
	start uint64
	nodes uint64
	logger *log.Logger
}

func NewManager(me, start, n uint64, logger *log.Logger) *Manager {
	m := &Manager{
		me: me,
		learned: make(chan Result),
		reqs: make(chan instReq),
		seqns: make(chan uint64),
		nodes: n,
		start: start,
		logger: logger,
	}
	return m
}

func (m *Manager) Init(outs Putter) {
	go func() {
		instances := make(map[uint64]*Instance)
		for req := range m.reqs {
			inst, ok := instances[req.seqn]
			if !ok {
				inst = NewInstance(m.me, m.nodes, m.logger)
				inst.Init(PutWrapper{req.seqn, 1, outs})
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
		for n := m.start; ; n++ {
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
	m.getInstance(msg.Seqn).Put(msg)
}

func (m *Manager) Propose(v string) string {
	seqn := <-m.seqns
	inst := m.getInstance(seqn)
	m.logger.Logf("paxos %d propose -> %q", seqn, v)
	inst.Propose(v)
	return inst.Value()
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	m.logger.Logf("paxos %d learned <- %q", result.seqn, result.v)
	return result.seqn, result.v
}

