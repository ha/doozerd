package lock

import (
	"junta"
	"junta/store"
	"junta/util"
	"strings"
)

type Lock struct {
	st *store.Store
	pp junta.Proposer
	ch chan store.Event
}

func New(st *store.Store, pp junta.Proposer) *Lock {
	ch := make(chan store.Event)
	st.Watch("/session/*", ch)

	lk := &Lock{st, pp, ch}
	go lk.process()
	return lk
}

func (lk *Lock) process() {
	logger := util.NewLogger("lock")

	for ev := range lk.ch {

		if ! ev.IsDel() {
			continue
		}

		parts := strings.Split(ev.Path, "/", 3)
		name := parts[2]
		logger.Logf("lost session %s", name)

		ch, err := store.Walk(lk.st, "/lock/**")
		if err != nil {
			continue
		}

		for ev := range ch {
			if ev.Body == name {
				mut, err := store.EncodeDel(ev.Path, store.Clobber)
				if err != nil {
					continue
				}

				// TODO:  Do we need to re-popropose if this doesn't work?
				lk.pp.Propose(mut)
			}
		}
	}
}

func (lk *Lock) Close() {
	close(lk.ch)
}
