package session

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/timer"
)

var sessions = store.MustCompileGlob("/session/*")

func Clean(s *store.Store, p paxos.Proposer) {
	timer := timer.New(sessions, timer.OneSecond, s)
	for tick := range timer.C {
		_, cas := s.Get(tick.Path)
		paxos.Del(p, tick.Path, cas, nil)
	}
}
