package mon

import (
	"junta/client"
	"junta/store"
	"junta/util"
	"log"
	"os"
	"path"
	"strings"
	"syscall"
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

type service struct {
	id, name string
	cmd  string
	ctl chan int
	st   *store.Store
	self, prefix string
	c    *client.Client
	logger *log.Logger
	mon *monitor
}

func newService(id, name string, mon *monitor) *service {
	sv := &service{
		id:   id,
		name: name,
		ctl:  make(chan int),
		st:   mon.st,
		self: mon.self,
		c:    mon.c,
		mon:  mon,
		logger: util.NewLogger(id),
		prefix: mon.prefix,
	}
	sv.logger.Log("new")
	go sv.run()
	return sv
}

func (sv *service) tryLock() {
	sv.c.Set(sv.prefix+lockKey+"/"+sv.id, sv.self, store.Missing)
}

func (sv *service) release(cas string) (string, int) {
	sv.mon.release(sv.id, cas)
	return store.Missing, inactive
}

func (sv *service) start(waitmsgCh chan *os.Waitmsg) (pid, state int) {
	sv.cmd = sv.lookupUnitParam("cmd")
	sv.logger.Log("starting")

	sv.logger.Log("*** *** *** RUN *** *** ***")
	args := strings.Split(sv.cmd, " ", -1)
	pid, err := os.ForkExec(args[0], args, nil, "", nil)
	if err != nil {
		sv.logger.Log(err)
		go sv.c.Set(sv.prefix+procKey+"/"+sv.id, err.String(), store.Clobber)
		return 0, error
	}

	go func() {
		w, err := os.Wait(pid, 0)
		if err != nil {
			sv.logger.Log(err)
			return
		}
		waitmsgCh <- w
	}()

	sv.setProcVal("port", "11300")
	return pid, running
}

func (sv *service) kill(pid int) {
	errno := syscall.Kill(pid, syscall.SIGTERM)
	if errno != 0 {
		sv.logger.Log(os.Errno(errno))
	}
}

func (sv *service) lookupUnitParam(param string) string {
	return sv.mon.lookupUnitParam(sv.id, param)
}

func (sv *service) setProcVal(param, val string) {
	sv.mon.setProcVal(sv.id, param, val)
}

func (sv *service) watchLock(ch chan store.Event) {
	sv.st.Watch(lockKey+"/"+sv.id, ch)
}

func isFatal(w *os.Waitmsg) bool {
	return w.Exited() && w.ExitStatus() != 0
}

func isError(w *os.Waitmsg) bool {
	return isFatal(w) || w.Signaled()
}

func (sv *service) run() {
	lockEvs := make(chan store.Event)
	waitmsgCh := make(chan *os.Waitmsg)
	state, cas := inactive, ""
	pid := 0

	sv.watchLock(lockEvs)

	for {
		select {
		case s := <-sv.ctl:
			switch s {
			case start:
				if state == inactive {
					sv.logger.Log("try lock")
					state = wantLock
					go sv.tryLock()
				}
			case stop:
				switch state {
				case wantLock:
					state = inactive
				case running:
					sv.logger.Log("stopping")
					sv.kill(pid)
					state = stopping
				}
			case destroy:
				// TODO stop the service and cleanly exit this goroutine
			}

		case ev := <-lockEvs:
			switch state {
			case wantLock:
				switch {
				case ev.Body == sv.self:
					sv.logger.Log("got lock")
					cas = ev.Cas
					pid, state = sv.start(waitmsgCh)
				case ev.Cas == store.Missing:
					sv.logger.Log("try lock")
					go sv.tryLock()
				}
			case running:
				// TODO we lost the lock? kill it
			default:
				// TODO make sure we don't unintentionally keep the lock
			}

		case w := <-waitmsgCh:
			if w.Pid != pid {
				break
			}
			pid = 0

			sv.logger.Log(w)
			if isFatal(w) {
				sv.logger.Log("fatal error -- giving up")
				go sv.c.Set(sv.prefix+procKey+"/"+sv.id, w.String(), store.Clobber)
				cas, _ = sv.release(cas)
				state = error
			}

			switch state {
			case running:
				if isError(w) {
					sv.logger.Log("non-fatal error -- restarting")
					go sv.c.Set(sv.prefix+procKey+"/"+sv.id, w.String(), store.Clobber)
				} else {
					sv.logger.Log("exited cleanly -- restarting")
				}
				pid, state = sv.start(waitmsgCh)
			case stopping:
				if isError(w) {
					sv.logger.Log("non-fatal error -- stopped")
					go sv.c.Set(sv.prefix+procKey+"/"+sv.id, w.String(), store.Clobber)
				} else {
					sv.logger.Log("exited cleanly -- stopped")
				}
				cas, state = sv.release(cas)
			case error:
			}
		}
	}
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
