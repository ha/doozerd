package lock

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/util"
	"strings"
)

func Clean(st *store.Store, pp paxos.Proposer) {
	logger := util.NewLogger("lock")
	for ev := range st.Watch("/session/*") {
		if !ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Printf("lost session %s", name)

		err := store.Walk(ev, "/lock/**", func(path, body, cas string) {
			if body == name {
				paxos.Del(pp, path, cas)
			}
		})
		if err != nil {
			logger.Println(err)
		}
	}
}
