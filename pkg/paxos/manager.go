package paxos

import (
	"log"
)

type Result struct {
	seqn uint64
	v    string
}

type instReq struct {
	seqn uint64 // 0 means to generate a fresh seqn
	cx   *cluster
	ch   chan *Instance
}

type Manager struct {
	self    string
	nodes   []string
	learned chan Result
	reqs    chan instReq
	logger  *log.Logger
}

func (m *Manager) process(next uint64, outs Putter) {
	instances := make(map[uint64]*Instance)
	for req := range m.reqs {
		if req.seqn == 0 {
			req.seqn = next
		}
		inst, ok := instances[req.seqn]
		if !ok {
			inst = NewInstance(req.cx, PutWrapper{req.seqn, 1, outs}, m.logger)
			instances[req.seqn] = inst
			go func() {
				m.learned <- Result{req.seqn, inst.Value()}
			}()
		}
		req.ch <- inst
		if req.seqn >= next {
			next = req.seqn + 1
		}
	}
}

func NewManager(start uint64, self string, nodes []string, outs Putter, logger *log.Logger) *Manager {
	m := &Manager{
		self:    self,
		nodes:   nodes,
		learned: make(chan Result),
		reqs:    make(chan instReq),
		logger:  logger,
	}

	go m.process(start, outs)

	return m
}

func (m *Manager) getInstance(seqn uint64) *Instance {
	ch := make(chan *Instance)
	// TODO read list of nodes from the data store
	cx := NewCluster(m.self, m.nodes)
	m.reqs <- instReq{seqn, cx, ch}
	return <-ch
}

func (m *Manager) Put(msg Message) {
	m.getInstance(msg.Seqn()).Put(msg)
}

func (m *Manager) Propose(v string) string {
	inst := m.getInstance(0)
	m.logger.Logf("paxos propose -> %q", v)
	inst.Propose(v)
	return inst.Value()
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	m.logger.Logf("paxos %d learned <- %q", result.seqn, result.v)
	return result.seqn, result.v
}
