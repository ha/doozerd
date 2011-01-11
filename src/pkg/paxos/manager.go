package paxos

import (
	"log"
	"os"

	"doozer/store"
	"doozer/util"
	"time"
)

const (
	fillDelay = 5e8 // 500ms
)

var ErrCancel = os.NewError("received cancel")

type instReq struct {
	seqn int64
	ch   chan instance
}

type Manager struct {
	st        *store.Store
	ops       chan<- store.Op
	rg        *Registrar
	seqns     chan int64
	fillUntil chan int64
	reqs      chan instReq
	logger    *log.Logger
	Self      string
	alpha     int
	outs      PutterTo
}

// start is the seqn at which this member was defined.
// start+alpha is the first seqn this manager is expected to participate in.
func NewManager(self string, alpha int, st *store.Store, outs PutterTo) *Manager {
	start := <-st.Seqns
	m := &Manager{
		st:        st,
		ops:       st.Ops,
		rg:        NewRegistrar(st, start, alpha),
		seqns:     make(chan int64),
		fillUntil: make(chan int64),
		reqs:      make(chan instReq),
		logger:    util.NewLogger("manager"),
		Self:      self,
		alpha:     alpha,
		outs:      outs,
	}

	go m.gen(start + int64(alpha))
	go m.fill(start + int64(alpha))
	go m.process()

	// Wait until process is ready
	// TODO: is there something we can do to avoid this?
	m.getInstance(0)

	return m
}

func (m *Manager) Alpha() int {
	return m.alpha
}

func (m *Manager) cluster(seqn int64) *cluster {
	members, cals := m.rg.setsForSeqn(seqn)
	return newCluster(m.Self, members, cals, putToWrapper{seqn, m.outs})
}

func (mg *Manager) gen(next int64) {
	for {
		cx := mg.cluster(next)
		leader := int(next % int64(cx.Len()))
		if leader == cx.SelfIndex() {
			mg.seqns <- next
		}
		next++
	}
}

func (mg *Manager) fill(seqn int64) {
	for next := range mg.fillUntil {
		for seqn < next {
			go mg.fillOne(seqn)
			seqn++
		}
		seqn = next + 1 // no need to fill in our own seqn
	}
}

func (m *Manager) process() {
	instances := make(map[int64]instance)
	var ver int64
	seqns := m.st.Watch(store.Any)
	for {
		select {
		case ev := <-seqns:
			if closed(seqns) {
				return
			}

			it, ok := instances[ev.Seqn]
			if ok {
				close(it)
				instances[ev.Seqn] = nil, false
			}
			ver = ev.Seqn
		case req := <-m.reqs:
			if closed(m.reqs) {
				return
			}

			inst, ok := instances[req.seqn]
			if !ok && req.seqn > ver {
				inst = make(instance)
				instances[req.seqn] = inst
				go inst.process(req.seqn, m, m.ops)
			}
			req.ch <- inst
		}
	}
}

func (m *Manager) getInstance(seqn int64) instance {
	ch := make(chan instance)
	m.reqs <- instReq{seqn, ch}
	return <-ch
}

func (m *Manager) PutFrom(addr string, msg Msg) {
	if !msg.Ok() {
		return
	}
	n := msg.Seqn()
	it := m.getInstance(n)
	if it == nil {
		ev := <-m.st.Wait(n)
		putToWrapper{n, m.outs}.PutTo(newLearn(ev.Mut), addr)
	} else {
		it.PutFrom(addr, msg)
	}
}

func (m *Manager) proposeAt(seqn int64, v string) {
	it := m.getInstance(seqn)
	if it != nil {
		it.Propose(v)
		m.logger.Printf("paxos propose -> %d %q", seqn, v)
	}
}

func (m *Manager) ProposeOnce(v string, cancel chan bool) store.Event {
	select {
	case seqn := <-m.seqns:
		ch := m.st.Wait(seqn)
		m.proposeAt(seqn, v)
		m.fillUntil <- seqn
		select {
		case e := <-ch:
			return e
		case <-cancel:
			return store.Event{Err: ErrCancel}
		}
	case <-cancel:
		return store.Event{Err: ErrCancel}
	}
	panic("not reached")
}

func (m *Manager) Propose(v string, cancel chan bool) (seqn int64, cas string, err os.Error) {
	var ev store.Event

	// If a competing proposal succeeded in the same seqn, we should try again.
	for v != ev.Mut {
		if cancel != nil {
			if _, ok := <-cancel; ok {
				return 0, "", ErrCancel
			}
		}
		ev = m.ProposeOnce(v, cancel)
	}
	return ev.Seqn, ev.Cas, ev.Err
}

func (m *Manager) fillOne(seqn int64) {
	time.Sleep(fillDelay)
	// yes, we'll act as coordinator for a seqn we don't "own"
	// this is intentional, since we want to exersize this code all the time,
	// not just during a failure.
	m.proposeAt(seqn, store.Nop)
}
