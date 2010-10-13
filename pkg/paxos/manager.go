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
	alpha   int
	outs    PutterTo
}

func NewManager(self string, start uint64, alpha int, st *store.Store, outs PutterTo) *Manager {
	m := &Manager{
		st:      st,
		rg:      NewRegistrar(st, start, alpha),
		learned: make(chan result),
		reqs:    make(chan *instReq),
		logger:  util.NewLogger("manager"),
		Self:    self,
		alpha:   alpha,
		outs:    outs,
	}

	go m.process(start+uint64(alpha))

	return m
}

func (m *Manager) Alpha() int {
	return m.alpha
}

func (m *Manager) cluster(seqn uint64) *cluster {
	members, cals := m.rg.setsForSeqn(seqn)
	return newCluster(m.Self, members, cals, putToWrapper{seqn, m.outs})
}

func (m *Manager) process(next uint64) {
	instances := make(map[uint64]*instance)
	for req := range m.reqs {
		if req.seqn == 0 {
			req.seqn = next
		}
		inst, ok := instances[req.seqn]
		if !ok {
			inst = newInstance(req.seqn, m)
			instances[req.seqn] = inst
			go func(seqn uint64, it *instance) {
				m.learned <- result{seqn, it.Value()}
			}(req.seqn, inst)
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

func (m *Manager) PutFrom(addr string, msg Msg) {
	if !msg.Ok() {
		return
	}
	_, it := m.getInstance(msg.Seqn())
	it.PutFrom(addr, msg)
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
