package member

import (
	"doozer/consensus"
	"doozer/store"
	"log"
)

var (
	slots = store.MustCompileGlob("/doozer/slot/*")
)

func Clean(c chan string, st *store.Store, p consensus.Proposer) {
	for addr := range c {
		_, g := st.Snap()
		name := getId(addr, g)

		if name != "" {
			go func() {
				clearSlot(p, g, name)
				removeMember(p, g, name)
				removeInfo(p, g, name)
			}()
		}
	}
}


func getId(addr string, g store.Getter) string {
	for _, slot := range store.Getdir(g, "/doozer/slot") {
		id := store.GetString(g, "/doozer/slot/"+slot)
		if store.GetString(g, "/doozer/members/"+id) == addr {
			return id
		}
	}
	return ""
}


func clearSlot(p consensus.Proposer, g store.Getter, name string) {
	store.Walk(g, slots, func(path, body string, rev int64) bool {
		if body == name {
			consensus.Set(p, path, nil, rev)
		}
		return false
	})
}

func removeMember(p consensus.Proposer, g store.Getter, name string) {
	k := "/doozer/members/" + name
	_, rev := g.Get(k)
	if rev != store.Missing {
		consensus.Del(p, k, rev)
	}
}

func removeInfo(p consensus.Proposer, g store.Getter, name string) {
	glob, err := store.CompileGlob("/doozer/info/" + name + "/**")
	if err != nil {
		log.Println(err)
		return
	}
	store.Walk(g, glob, func(path, _ string, rev int64) bool {
		consensus.Del(p, path, rev)
		return false
	})
}
