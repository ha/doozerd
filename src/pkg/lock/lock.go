package lock

import (
	"doozer/consensus"
	"doozer/store"
	"doozer/util"
	"strings"
)

var (
	sessions = store.MustCompileGlob("/session/*")
	locks    = store.MustCompileGlob("/lock/**")
)

func Clean(st *store.Store, pp consensus.Proposer) {
	logger := util.NewLogger("lock")
	for ev := range st.Watch(sessions) {
		if !ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Printf("lost session %s", name)

		store.Walk(ev, locks, func(path, body string, cas int64) bool {
			if body == name {
				consensus.Del(pp, path, cas)
			}
			return false
		})
	}
}
