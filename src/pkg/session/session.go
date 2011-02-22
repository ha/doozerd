package session

import (
	"doozer/consensus"
	"doozer/store"
	"doozer/timer"
)

var sessions = store.MustCompileGlob("/session/*")

func Clean(s *store.Store, p consensus.Proposer) {
	timer := timer.New(sessions, timer.OneSecond, s)
	for tick := range timer.C {
		consensus.Del(p, tick.Path, tick.Cas)
	}
}
