package member

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/util"
	"strings"
)

var logger = util.NewLogger("member")

func Clean(st *store.Store, p paxos.Proposer) {
	for ev := range st.Watch("/session/*") {
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
	err := store.Walk(g, "/doozer/slot/*", func(path, body, cas string) {
		if body == name {
			paxos.Set(p, path, "", cas)
		}
	})
	if err != nil {
		logger.Println(err)
	}
}

func removeMember(p paxos.Proposer, g store.Getter, name string) {
	k := "/doozer/members/" + name
	_, cas := g.Get(k)
	if cas != store.Missing {
		paxos.Del(p, k, cas)
	}
}

func removeInfo(p paxos.Proposer, g store.Getter, name string) {
	err := store.Walk(g, "/doozer/info/"+name+"/**", func(path, _, cas string) {
		paxos.Del(p, path, cas)
	})
	if err != nil {
		logger.Println(err)
	}
}
