package paxos

import (
	"log"
	"os"

	"junta/store"
	"junta/util"
)

const window = 50

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
				return m.rg.clusterFor(req.seqn)
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

func NewManager(start uint64, alpha int, st *store.Store, outs Putter) *Manager {
	self := "a"
	m := &Manager{
		st:      st,
		rg:      NewRegistrar(self, st, alpha),
		learned: make(chan result),
		reqs:    make(chan *instReq),
		logger:  util.NewLogger("manager"),
		Self:    self,
	}

	go m.process(start, outs)

	return m
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

func (m *Manager) Propose(v string) (string, os.Error) {
	ch := make(chan store.Status)
	seqn, inst := m.getInstance(0)
	m.st.Wait(seqn, ch)
	m.logger.Logf("paxos propose -> %q", v)
	inst.Propose(v)
	status := <-ch
	return status.M, status.Err
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	m.logger.Logf("paxos %d learned <- %q", result.seqn, result.v)
	return result.seqn, result.v
}
