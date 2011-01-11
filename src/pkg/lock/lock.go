package lock

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/util"
	"strings"
)

var (
	sessions = store.MustCompileGlob("/session/*")
	locks    = store.MustCompileGlob("/lock/**")
)

func Clean(st *store.Store, pp paxos.Proposer) {
	logger := util.NewLogger("lock")
	for ev := range st.Watch(sessions) {
		if !ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Printf("lost session %s", name)

		store.Walk(ev, locks, func(path, body, cas string) bool {
			if body == name {
				paxos.Del(pp, path, cas, nil)
			}
			return false
		})
	}
}
