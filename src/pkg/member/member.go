package member

import (
	"doozer/consensus"
	"doozer/store"
	"doozer/util"
)

var logger = util.NewLogger("member")

var (
	slots = store.MustCompileGlob("/doozer/slot/*")
)

func Clean(c chan string, st *store.Store, p consensus.Proposer) {
	for addr := range c {
		_, g := st.Snap()
		name := getId(addr, g)

		if name != "" {
			logger.Printf("lost session %s", name)
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
	store.Walk(g, slots, func(path, body string, cas int64) bool {
		if body == name {
			consensus.Set(p, path, nil, cas)
		}
		return false
	})
}

func removeMember(p consensus.Proposer, g store.Getter, name string) {
	k := "/doozer/members/" + name
	_, cas := g.Get(k)
	if cas != store.Missing {
		consensus.Del(p, k, cas)
	}
}

func removeInfo(p consensus.Proposer, g store.Getter, name string) {
	glob, err := store.CompileGlob("/doozer/info/" + name + "/**")
	if err != nil {
		logger.Println(err)
		return
	}
	store.Walk(g, glob, func(path, _ string, cas int64) bool {
		consensus.Del(p, path, cas)
		return false
	})
}
