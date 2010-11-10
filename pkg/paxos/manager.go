package paxos

import (
	"log"
	"os"

	"junta/store"
	"junta/util"
	"time"
)

const (
	fillDelay = 5e8 // 500ms
)

type instReq struct {
	seqn uint64
	ch   chan instance
}

type Manager struct {
	st        *store.Store
	ops       chan<- store.Op
	rg        *Registrar
	seqns     chan uint64
	fillUntil chan uint64
	reqs      chan instReq
	logger    *log.Logger
	Self      string
	alpha     int
	outs      PutterTo
}

// start is the seqn at which this member was defined.
// start+alpha is the first seqn this manager is expected to participate in.
func NewManager(self string, start uint64, alpha int, st *store.Store, ops chan<- store.Op, outs PutterTo) *Manager {
	m := &Manager{
		st:        st,
		ops:       ops,
		rg:        NewRegistrar(st, start, alpha),
		seqns:     make(chan uint64),
		fillUntil: make(chan uint64),
		reqs:      make(chan instReq),
		logger:    util.NewLogger("manager"),
		Self:      self,
		alpha:     alpha,
		outs:      outs,
	}

	go m.gen(start + uint64(alpha))
	go m.fill(start + uint64(alpha))
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
		cx := mg.cluster(next)
		leader := int(next % uint64(cx.Len()))
		if leader == cx.SelfIndex() {
			mg.seqns <- next
		}
		next++
	}
}

func (mg *Manager) fill(seqn uint64) {
	for next := range mg.fillUntil {
		for seqn < next {
			go mg.fillOne(seqn)
			seqn++
		}
		seqn = next + 1 // no need to fill in our own seqn
	}
}

func (m *Manager) process() {
	instances := make(map[uint64]instance)
	for req := range m.reqs {
		inst, ok := instances[req.seqn]
		if !ok {
			inst = make(instance)
			instances[req.seqn] = inst
			go inst.process(req.seqn, m, m.ops)
		}
		req.ch <- inst
	}
}

func (m *Manager) getInstance(seqn uint64) instance {
	ch := make(chan instance)
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

func (m *Manager) proposeAt(seqn uint64, v string) {
	m.getInstance(seqn).Propose(v)
	m.logger.Printf("paxos propose -> %d %q", seqn, v)
}

func (m *Manager) Propose(v string) (seqn uint64, cas string, err os.Error) {
	var ev store.Event

	// If a competing proposal succeeded in the same seqn, we should try again.
	for v != ev.Mut {
		seqn = <-m.seqns
		ch := m.st.Wait(seqn)
		m.proposeAt(seqn, v)
		m.fillUntil <- seqn
		ev = <-ch
	}
	return seqn, ev.Cas, ev.Err
}

func (m *Manager) fillOne(seqn uint64) {
	time.Sleep(fillDelay)
	// yes, we'll act as coordinator for a seqn we don't "own"
	// this is intentional, since we want to exersize this code all the time,
	// not just during a failure.
	m.proposeAt(seqn, store.Nop)
}
