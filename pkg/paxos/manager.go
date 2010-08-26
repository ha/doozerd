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
	self string
	nodes []string
	learned chan Result
	reqs chan instReq
	seqns chan uint64
	start uint64
	logger *log.Logger
}

func NewManager(start uint64, self string, nodes []string, outs Putter, logger *log.Logger) *Manager {
	m := &Manager{
		self: self,
		nodes: nodes,
		learned: make(chan Result),
		reqs: make(chan instReq),
		seqns: make(chan uint64),
		start: start,
		logger: logger,
	}
	go func() {
		instances := make(map[uint64]*Instance)
		for req := range m.reqs {
			inst, ok := instances[req.seqn]
			if !ok {
				// TODO read list of nodes from the data store
				cx := NewCluster(m.self, m.nodes, PutWrapper{req.seqn, 1, outs})
				inst = NewInstance(cx, m.logger)
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

	return m
}

func (m *Manager) getInstance(seqn uint64) *Instance {
	ch := make(chan *Instance)
	m.reqs <- instReq{seqn, ch}
	return <-ch
}

func (m *Manager) Put(msg Message) {
	m.getInstance(msg.Seqn()).Put(msg)
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

