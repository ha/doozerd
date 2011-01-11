package member

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/util"
	"strings"
)

var logger = util.NewLogger("member")

var (
	sessions = store.MustCompileGlob("/session/*")
	slots    = store.MustCompileGlob("/doozer/slot/*")
)

func Clean(st *store.Store, p paxos.Proposer) {
	for ev := range st.Watch(sessions) {
		if !ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Printf("lost session %s", name)

		go func() {
			clearSlot(p, ev, name)
			removeMember(p, ev, name)
			removeInfo(p, ev, name)
		}()
	}
}

func clearSlot(p paxos.Proposer, g store.Getter, name string) {
	store.Walk(g, slots, func(path, body, cas string) bool {
		if body == name {
			paxos.Set(p, path, "", cas, nil)
		}
		return false
	})
}

func removeMember(p paxos.Proposer, g store.Getter, name string) {
	k := "/doozer/members/" + name
	_, cas := g.Get(k)
	if cas != store.Missing {
		paxos.Del(p, k, cas, nil)
	}
}

func removeInfo(p paxos.Proposer, g store.Getter, name string) {
	glob, err := store.CompileGlob("/doozer/info/"+name+"/**")
	if err != nil {
		logger.Println(err)
		return
	}
	store.Walk(g, glob, func(path, _, cas string) bool {
		paxos.Del(p, path, cas, nil)
		return false
	})
}
