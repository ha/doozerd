package gc

import (
	"doozer/store"
	"doozer/util"
	"log"
	"strings"
	"strconv"
)

var applied = store.MustCompileGlob("/doozer/info/*/applied")

type cleaner struct {
	st     *store.Store
	table  map[string]int64
	logger *log.Logger
}

func Clean(st *store.Store) {
	cl := &cleaner{
		st:     st,
		table:  make(map[string]int64),
		logger: util.NewLogger("clean"),
	}

	for ev := range st.Watch(applied) {
		cl.update(ev)
		cl.check()
	}
}

func (cl *cleaner) update(ev store.Event) {
	parts := strings.Split(ev.Path, "/", -1)
	id := parts[3]
	seqn, err := strconv.Atoi64(ev.Body)
	if err != nil {
		cl.logger.Println(err)
		return
	}
	cl.table[id] = seqn
}

func (cl *cleaner) check() {
	for _, seqn := range cl.table {
		if cl.isOk(seqn) {
			cl.st.Clean(seqn)
		}
	}
}

func (cl *cleaner) isOk(seqn int64) bool {
	for _, c := range cl.getCals(seqn) {
		if cl.table[c] < seqn {
			return false
		}
	}
	return true
}

func (cl *cleaner) getCals(seqn int64) []string {
	slots := store.GetDir(cl.st, "/doozer/slot")
	cals := make([]string, len(slots))
	for i, slot := range slots {
		cals[i] = store.GetString(cl.st, "/doozer/slot/"+slot)
	}
	return cals
}
