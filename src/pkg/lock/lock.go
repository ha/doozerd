package lock

import (
	"doozer/consensus"
	"doozer/store"
)

const SessDir = "/ctl/sess"

var (
	SessGlob = store.MustCompileGlob(SessDir + "/*")
	locks    = store.MustCompileGlob("/lock/**")
)

func Clean(p consensus.Proposer, ch <-chan store.Event) {
	for ev := range ch {
		if ev.IsDel() {
			name := ev.Path[len(SessDir)+1:]

			store.Walk(ev, locks, func(path, body string, rev int64) bool {
				if body == name {
					go consensus.Del(p, path, rev)
				}
				return false
			})
		}
	}
}
