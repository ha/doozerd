package member

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/util"
	"time"
)

var logger = util.NewLogger("member")

var (
	slots    = store.MustCompileGlob("/doozer/slot/*")
)

func Clean(c chan string, st *store.Store, p paxos.Proposer) {
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
	for _, slot := range store.GetDir(g, "/doozer/slot") {
		id := store.GetString(g, "/doozer/slot/"+slot)
		if store.GetString(g, "/doozer/members/"+id) == addr {
			return id
		}
	}
	return ""
}


func clearSlot(p paxos.Proposer, g store.Getter, name string) {
	store.Walk(g, slots, func(path, body string, cas int64) bool {
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
	store.Walk(g, glob, func(path, _ string, cas int64) bool {
		paxos.Del(p, path, cas, nil)
		return false
	})
}


func Timeout(live, shun chan string, timeout int64) {
	ticker := time.Tick(timeout / 10)
	times := make(map[string]int64)
	for {
		select {
		case addr := <-live: // got a packet
			times[addr] = time.Nanoseconds()
		case t := <-ticker:
			n := t - timeout
			for addr, s := range times {
				if n > s {
					times[addr] = 0, false
					shun <- addr
				}
			}
		}
	}
}
