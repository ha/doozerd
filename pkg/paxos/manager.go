package paxos

import (
	"log"
	"os"

	"junta/store"
	"junta/util"
)

type result struct {
	seqn uint64
	v    string
}

type instReq struct {
	seqn uint64 // 0 means to fill in a fresh seqn
	ch   chan *instance
}

type Manager struct {
	st      *store.Store
	rg      *Registrar
	learned chan result
	reqs    chan *instReq
	logger  *log.Logger
	Self    string
	Alpha   int
}

func NewManager(self string, start uint64, alpha int, st *store.Store, outs Putter) *Manager {
	m := &Manager{
		st:      st,
		rg:      NewRegistrar(st, start, alpha),
		learned: make(chan result),
		reqs:    make(chan *instReq),
		logger:  util.NewLogger("manager"),
		Self:    self,
		Alpha:   alpha,
	}

	go m.process(start+uint64(alpha), outs)

	return m
}

func (m *Manager) process(next uint64, outs Putter) {
	instances := make(map[uint64]*instance)
	for req := range m.reqs {
		if req.seqn == 0 {
			req.seqn = next
		}
		inst, ok := instances[req.seqn]
		if !ok {
			// TODO find a nicer way to do this
			// This is meant to be run in a separate goroutine
			cxf := func() *cluster {
				ms, active := m.rg.setsForSeqn(req.seqn)
				m.logger.Logf("cluster %d has %d members and %d active", req.seqn, len(ms), len(active))
				m.logger.Logf("  members: %v", ms)
				m.logger.Logf("  active: %v", active)
				return newCluster(m.Self, ms, active)
			}
			inst = newInstance(cxf, putWrapper{req.seqn, outs})
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

func (m *Manager) getInstance(seqn uint64) (uint64, *instance) {
	r := &instReq{seqn, make(chan *instance)}
	m.reqs <- r
	it := <-r.ch
	return r.seqn, it
}

func (m *Manager) Put(msg Msg) {
	if !msg.Ok() {
		return
	}
	_, it := m.getInstance(msg.Seqn())
	it.Put(msg)
}

func (m *Manager) PutFrom(addr string, msg Msg) {
	_, it := m.getInstance(msg.Seqn())
	msg.SetFrom(it.cluster().indexByAddr(addr))
	m.Put(msg)
}

func (m *Manager) AddrsFor(msg Msg) []string {
	_, it := m.getInstance(msg.Seqn())
	return it.cluster().addrs()
}

func (m *Manager) Propose(v string) (uint64, string, os.Error) {
	ch := make(chan store.Event)
	seqn, inst := m.getInstance(0)
	m.st.Wait(seqn, ch)
	m.logger.Logf("paxos propose -> %q", v)
	inst.Propose(v)
	ev := <-ch
	return seqn, ev.Mut, ev.Err
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	m.logger.Logf("paxos %d learned <- %q", result.seqn, result.v)
	return result.seqn, result.v
}
