package mon

import (
	"junta/store"
	"junta/util"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

const (
	ctlKey    = "/mon/ctl"
	defKey    = "/mon/def"
	lockKey   = "/lock"
	statusKey = "/mon/status"

	ctlDir    = ctlKey + "/"
	defDir    = defKey + "/"
	lockDir   = lockKey + "/"
	statusDir = statusKey + "/"
)

type exit struct {
	u unit
	w *os.Waitmsg
}

type unit interface {
	dispatchLockEvent(ev store.Event)
	start()
	stop()
	tick()
	exited(w *os.Waitmsg)
}

type SetDeler interface {
	Set(p, body, cas string) (seqn uint64, err os.Error)
	Del(p, cas string) (seqn uint64, err os.Error)
}

type monitor struct {
	self, prefix string
	host         string
	st           *store.Store
	cl           SetDeler
	clock        chan unit
	units        map[string]unit
	exitCh       chan exit
	logger       *log.Logger
}

func splitId(id string) (name, ext string) {
	parts := strings.Split(id, ".", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func Monitor(self, prefix string, st *store.Store, cl SetDeler) os.Error {

	mon := &monitor{
		self:   self,
		prefix: prefix,
		host:   os.Getenv("HOSTNAME"),
		st:     st,
		cl:     cl,
		clock:  make(chan unit),
		units:  make(map[string]unit),
		exitCh: make(chan exit),
		logger: util.NewLogger("monitor"),
	}

	mon.logger.Log("reading units")
	evs := make(chan store.Event)
	st.Watch(ctlKey+"/*", evs)
	st.Watch(lockKey+"/*", evs)
	go func() {
		for _, id := range st.LookupDir(ctlKey) {
			p := ctlDir + id
			v, cas := st.Lookup(p)
			if cas != store.Dir && cas != store.Missing {
				mon.logger.Log("injecting", id)
				evs <- store.Event{0, p, v[0], cas, "", nil}
			}
		}
	}()

	for {
		select {
		case u := <-mon.clock:
			u.tick()
		case ev := <-evs:
			switch {
			case strings.HasPrefix(ev.Path, ctlDir):
				id := splitPath(ev.Path)

				if ev.IsDel() {
					ut := mon.units[id]
					if ut != nil {
						ut.stop()
					}
					mon.units[id] = nil, false
					break
				}

				ut := mon.units[id]
				if ut == nil {
					ut = mon.newUnit(id)
					mon.units[id] = ut
				}

				switch ev.Body {
				case "start":
					ut.start()
				case "stop":
					ut.stop()
				case "auto", "":
					fallthrough
				default:
					// nothing
				}

			case strings.HasPrefix(ev.Path, lockDir):
				id := splitPath(ev.Path)

				ut := mon.units[id]
				if ut == nil {
					break
				}

				ut.dispatchLockEvent(ev)
			}
		case e := <-mon.exitCh:
			e.u.exited(e.w)
		}
	}
	panic("unreachable")
}

func (mon *monitor) newUnit(id string) unit {
	name, ext := splitId(id)
	switch ext {
	case "service":
		return newService(id, name, mon)
	case "socket":
		panic("TODO")
	}
	return nil
}

func (mon *monitor) lookupParam(id, param string) string {
	return mon.st.LookupString(defDir + id + "/" + param)
}

func (mon *monitor) setStatus(id, param, val string) {
	mon.cl.Set(mon.prefix+statusKey+"/"+id+"/"+param, val, store.Clobber)
}

func (mon *monitor) delStatus(id, param string) {
	mon.cl.Del(mon.prefix+statusKey+"/"+id+"/"+param, store.Clobber)
}

func (mon *monitor) tryLock(id string) {
	mon.cl.Set(mon.prefix+lockKey+"/"+id, mon.self, store.Missing)
}

func (mon *monitor) release(id, cas string) {
	mon.cl.Del(mon.prefix+lockKey+"/"+id, cas)
}

// `p` should look like `/foo/bar/name.ext
func splitPath(p string) (id string) {
	_, id = path.Split(p)
	return
}

func (mon *monitor) timer(u unit, ns int64) {
	time.Sleep(ns)
	mon.clock <- u
}

func (mon *monitor) wait(pid int, u unit) {
	w, err := os.Wait(pid, 0)
	if err != nil {
		mon.logger.Log(err)
		return
	}

	mon.exitCh <- exit{u, w}
}
