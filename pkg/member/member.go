package member

import (
	"junta/paxos"
	"junta/store"
	"junta/util"
	"strings"
)

// TODO remove this type entirely once store.Close is implemented
type Cleaner struct {
	st *store.Store
	p  paxos.Proposer
	ch chan store.Event
}

func New(st *store.Store, p paxos.Proposer) *Cleaner {
	ch := make(chan store.Event)
	st.Watch("/session/*", ch)

	mb := &Cleaner{st, p, ch}
	go mb.process()
	return mb
}

func (mb *Cleaner) process() {
	logger := util.NewLogger("member")

	for ev := range mb.ch {
		if !ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Printf("lost session %s", name)

		mb.clearSlot(ev, name)
		mb.removeMember(ev, name)
		mb.removeInfo(ev, name)
	}
}

func (mb *Cleaner) Close() {
	close(mb.ch)
}

func (mb *Cleaner) clearSlot(g store.Getter, name string) {
	ch, err := store.Walk(g, "/junta/slot/*")
	if err != nil {
		panic(err)
	}

	for ev := range ch {
		if ev.Body == name {
			paxos.Set(mb.p, ev.Path, "", ev.Cas)
		}
	}
}

func (mb *Cleaner) removeMember(g store.Getter, name string) {
	p := "/junta/members/" + name
	_, cas := g.Get(p)
	if cas != store.Missing {
		paxos.Del(mb.p, p, cas)
	}
}

func (mb *Cleaner) removeInfo(g store.Getter, name string) {
	ch, err := store.Walk(g, "/junta/info/"+name+"/**")
	if err != nil {
		panic(err)
	}

	for ev := range ch {
		paxos.Del(mb.p, ev.Path, ev.Cas)
	}
}
