package mon

import (
	"junta/client"
	"junta/store"
	"junta/util"
	"os"
	"path"
	"strings"
)

const (
	unitKey = "/mon/unit"
	lockKey = "/mon/lock"
	procKey = "/mon/proc"
)

const (
	start = iota
	stop
	destroy
)

const (
	inactive = iota
	wantLock
	running
	stopping
	error
)

func isFatal(w *os.Waitmsg) bool {
	return w.Exited() && w.ExitStatus() != 0
}

func isError(w *os.Waitmsg) bool {
	return isFatal(w) || w.Signaled()
}

type monitor struct {
	self, prefix string
	st   *store.Store
	c    *client.Client
}

func splitId(id string) (name, ext string) {
	parts := strings.Split(id, ".", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func Monitor(self, prefix string, st *store.Store) os.Error {
	logger := util.NewLogger("monitor")
	v, cas := st.Lookup("/junta/leader")
	if cas == store.Dir || cas == store.Missing {
		return os.NewError("boo") // TODO do this right
	}
	leader := v[0]

	v, cas = st.Lookup("/junta/members/"+leader)
	if cas == store.Dir || cas == store.Missing {
		return os.NewError("boo") // TODO do this right
	}
	leaderAddr := v[0]

	c, err := client.Dial(leaderAddr)
	if err != nil {
		return err
	}

	mon := &monitor{self, prefix, st, c}

	logger.Log("reading services")
	evs := make(chan store.Event)
	st.Watch(unitKey + "/*/start", evs)
	st.Watch(unitKey + "/*/created-at", evs)
	names, cas := st.Lookup(unitKey)
	if cas == store.Dir {
		for _, id := range names {
			path := unitKey + "/" + id + "/start"
			v, cas := st.Lookup(path)
			if cas != store.Dir && cas != store.Missing {
				logger.Log("injecting", id)
				go func(e store.Event) {
					evs <- e
				}(store.Event{0, path, v[0], cas, "", nil})
			}
		}
	}

	services := make(map[string]*service)
	for ev := range evs {
		p, param := path.Split(ev.Path)
		_, id := path.Split(p[0:len(p)-1])
		s, ok := services[id]
		switch param {
		case "start":
			switch {
			case ev.IsSet():
				logger.Log("got start event")
				if !ok {
					logger.Log("creating service")
					name, ext := splitId(id)
					switch ext {
					case "service":
						s, ok = newService(id, name, mon), true
					default:
						logger.Log("unknown service type in", id)
					}
				}
				s.ctl <- start
				break
			case ev.IsDel() && ok:
				logger.Log("got stop event")
				s.ctl <- stop
			}
		case "created-at":
			if ev.IsDel() && ok {
				s.ctl <- destroy
				s, ok = nil, false
			}
		}
		services[id] = s, ok
	}
	panic("unreachable")
}

func (mon *monitor) lookupUnitParam(id, param string) string {
	return mon.st.LookupString(unitKey+"/"+id+"/"+param)
}

func (mon *monitor) setProcVal(id, param, val string) {
	mon.c.Set(mon.prefix+procKey+"/"+id+"/"+param, val, store.Clobber)
}

func (mon *monitor) release(id, cas string) {
	mon.c.Del(mon.prefix+lockKey+"/"+id, cas)
}
