package paxos

import (
	"borg/store"
	"log"
)

type result struct {
	seqn uint64
	v    string
}

type instReq struct {
	seqn uint64 // 0 means to generate a fresh seqn
	cver uint64
	ch   chan *instance
}

type Manager struct {
	self    string
	st      *store.Store
	learned chan result
	reqs    chan instReq
	logger  *log.Logger
}

func (m *Manager) process(next uint64, outs Putter) {
	instances := make(map[uint64]*instance)
	for req := range m.reqs {
		if req.seqn == 0 {
			req.seqn = next
		}
		inst, ok := instances[req.seqn]
		if !ok {
			inst = newInstance(m.self, m.st, req.cver, putWrapper{req.seqn, 1, outs}, m.logger)
			instances[req.seqn] = inst
			go func() {
				m.learned <- result{req.seqn, inst.Value()}
			}()
		}
		req.ch <- inst
		if req.seqn >= next {
			next = req.seqn + 1
		}
	}
}

func NewManager(start uint64, self string, st *store.Store, outs Putter, logger *log.Logger) *Manager {
	m := &Manager{
		self:    self,
		st:      st,
		learned: make(chan result),
		reqs:    make(chan instReq),
		logger:  logger,
	}

	go m.process(start, outs)

	return m
}

func (m *Manager) getInstance(seqn, cver uint64) *instance {
	ch := make(chan *instance)
	m.reqs <- instReq{seqn, cver, ch}
	return <-ch
}

func (m *Manager) Put(msg Msg) {
	if !msg.Ok() {
		return
	}
	m.getInstance(msg.Seqn(), msg.ClusterVersion()).Put(msg)
}

func (m *Manager) Propose(v string) string {
	inst := m.getInstance(0, 0)
	m.logger.Logf("paxos propose -> %q", v)
	inst.Propose(v)
	return inst.Value()
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	m.logger.Logf("paxos %d learned <- %q", result.seqn, result.v)
	return result.seqn, result.v
}
