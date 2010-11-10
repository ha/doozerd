package member

import (
	"junta/paxos"
	"junta/store"
	"junta/util"
	"strings"
)

func Clean(s *store.Store, p paxos.Proposer) {
	ch := make(chan store.Event)
	s.Watch("/session/*", ch)
	logger := util.NewLogger("member")

	for ev := range ch {
		if !ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Printf("lost session %s", name)

		clearSlot(p, ev, name)
		removeMember(p, ev, name)
		removeInfo(p, ev, name)
	}
}

func clearSlot(p paxos.Proposer, g store.Getter, name string) {
	ch, err := store.Walk(g, "/junta/slot/*")
	if err != nil {
		panic(err)
	}

	for ev := range ch {
		if ev.Body == name {
			paxos.Set(p, ev.Path, "", ev.Cas)
		}
	}
}

func removeMember(p paxos.Proposer, g store.Getter, name string) {
	k := "/junta/members/" + name
	_, cas := g.Get(k)
	if cas != store.Missing {
		paxos.Del(p, k, cas)
	}
}

func removeInfo(p paxos.Proposer, g store.Getter, name string) {
	ch, err := store.Walk(g, "/junta/info/"+name+"/**")
	if err != nil {
		panic(err)
	}

	for ev := range ch {
		paxos.Del(p, ev.Path, ev.Cas)
	}
}
