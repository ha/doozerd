package session

import (
	"junta/paxos"
	"junta/store"
	"junta/timer"
)

func Clean(s *store.Store, p paxos.Proposer) {
	timer := timer.New("/session/**", timer.OneSecond, s)
	for tick := range timer.C {
		_, cas := s.Get(tick.Path)
		paxos.Del(p, tick.Path, cas)
	}
}
