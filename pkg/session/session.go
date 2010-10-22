package session

import (
	"junta"
	"junta/store"
	"junta/timer"
)

type Session struct {
	st *store.Store
	pp junta.Proposer
	timer *timer.Timer
}

func New(st *store.Store, pp junta.Proposer) *Session {
	timer := timer.New("/session/**", timer.OneSecond, st)
	ss := &Session{st, pp, timer}
	go ss.process()
	return ss
}

func (ss *Session) process() {
	for tick := range ss.timer.C {
		mut, err := store.EncodeDel(tick.Path, store.Clobber)
		if err != nil {
			continue
		}
		ss.pp.Propose(mut)
	}
}

func (ss *Session) Close() {
	ss.timer.Close()
}
