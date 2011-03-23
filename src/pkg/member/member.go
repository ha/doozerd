package member

import (
	"doozer/consensus"
	"doozer/store"
	"log"
)

var (
	calGlob = store.MustCompileGlob("/ctl/cal/*")
)

func Clean(c chan string, st *store.Store, p consensus.Proposer) {
	for addr := range c {
		_, g := st.Snap()
		name := getId(addr, g)

		if name != "" {
			go func() {
				clearSlot(p, g, name)
				removeInfo(p, g, name)
			}()
		}
	}
}


func getId(addr string, g store.Getter) string {
	for _, cal := range store.Getdir(g, "/ctl/cal") {
		id := store.GetString(g, "/ctl/cal/"+cal)
		if store.GetString(g, "/ctl/node/"+id+"/addr") == addr {
			return id
		}
	}
	return ""
}


func clearSlot(p consensus.Proposer, g store.Getter, name string) {
	store.Walk(g, calGlob, func(path, body string, rev int64) bool {
		if body == name {
			consensus.Set(p, path, nil, rev)
		}
		return false
	})
}

func removeInfo(p consensus.Proposer, g store.Getter, name string) {
	glob, err := store.CompileGlob("/ctl/node/" + name + "/**")
	if err != nil {
		log.Println(err)
		return
	}
	store.Walk(g, glob, func(path, _ string, rev int64) bool {
		consensus.Del(p, path, rev)
		return false
	})
}
