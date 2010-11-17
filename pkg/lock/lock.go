package lock

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/util"
	"strings"
)

func Clean(st *store.Store, pp paxos.Proposer) {
	ch := make(chan store.Event)
	st.Watch("/session/*", ch)
	logger := util.NewLogger("lock")

	for ev := range ch {
		if !ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Printf("lost session %s", name)

		ch, err := store.Walk(ev, "/lock/**")
		if err != nil {
			continue
		}

		for ev := range ch {
			if ev.Body == name {
				paxos.Del(pp, ev.Path, ev.Cas)
			}
		}
	}
}
