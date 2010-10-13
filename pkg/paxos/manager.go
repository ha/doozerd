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
	seqns   chan uint64
	reqs    chan instReq
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
		seqns:   make(chan uint64),
		reqs:    make(chan instReq),
		logger:  util.NewLogger("manager"),
		Self:    self,
		alpha:   alpha,
		outs:    outs,
	}

	go m.gen(start+uint64(alpha))
	go m.process()

	return m
}

func (m *Manager) Alpha() int {
	return m.alpha
}

func (m *Manager) cluster(seqn uint64) *cluster {
	members, cals := m.rg.setsForSeqn(seqn)
	return newCluster(m.Self, members, cals, putToWrapper{seqn, m.outs})
}

func (mg *Manager) gen(next uint64) {
	for {
		mg.seqns <- next
		next++
	}
}

func (m *Manager) process() {
	instances := make(map[uint64]*instance)
	for req := range m.reqs {
		inst, ok := instances[req.seqn]
		if !ok {
			inst = newInstance(req.seqn, m)
			instances[req.seqn] = inst
			go func(seqn uint64, it *instance) {
				m.learned <- result{seqn, it.Value()}
			}(req.seqn, inst)
		}
		req.ch <- inst
	}
}

func (m *Manager) getInstance(seqn uint64) *instance {
	ch := make(chan *instance)
	m.reqs <- instReq{seqn, ch}
	return <-ch
}

func (m *Manager) PutFrom(addr string, msg Msg) {
	if !msg.Ok() {
		return
	}
	it := m.getInstance(msg.Seqn())
	it.PutFrom(addr, msg)
}

func (m *Manager) Propose(v string) (uint64, string, os.Error) {
	ch := make(chan store.Event)
	seqn := <-m.seqns
	inst := m.getInstance(seqn)
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
