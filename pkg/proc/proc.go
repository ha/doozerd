package proc

import (
	"junta/client"
	"junta/store"
	"junta/util"
	"log"
	"os"
	"path"
	"strings"
)

const (
	serviceKey = "/j/proc/service"
	lockKey = "/j/proc/lock"
	runKey = "/j/proc/run"
)

type service struct {
	name string
	cmd  string
	done chan int
	st   *store.Store
	self, prefix string
	c    *client.Client
	logger *log.Logger

	lockCas string
}

func newService(name, cmd string, mon *monitor) *service {
	sv := &service{
		name: name,
		cmd:  cmd,
		done: make(chan int),
		st:   mon.st,
		self: mon.self,
		c:    mon.c,
		logger: util.NewLogger(name),
		prefix: mon.prefix,
	}
	sv.logger.Log("new")
	go sv.run()
	return sv
}

func (sv *service) tryLock() {
	sv.c.Set(sv.prefix+lockKey+"/"+sv.name, sv.self, store.Missing)
}

func (sv *service) acquire() {
	evs := make(chan store.Event)
	sv.st.Watch(lockKey+"/"+sv.name, evs)
	go sv.tryLock()
	for ev := range evs {
		if ev.Body == sv.self {
			sv.lockCas = ev.Cas
			return
		}
		if ev.Cas == store.Missing {
			go sv.tryLock()
		}
	}
}

func (sv *service) release() {
	sv.c.Del(sv.prefix+lockKey+"/"+sv.name, sv.lockCas)
}

func (sv *service) once() bool {
	sv.acquire()
	defer sv.release()

	sv.logger.Log("*** *** *** RUN *** *** ***")
	args := strings.Split(sv.cmd, " ", -1)
	pid, err := os.ForkExec(args[0], args, nil, "", nil)
	if err != nil {
		sv.logger.Log(err)
		go sv.c.Set(sv.prefix+runKey+"/"+sv.name, err.String(), store.Clobber)
		return false
	}

	go sv.c.Set(sv.prefix+runKey+"/"+sv.name, ":11300", store.Clobber)

	w, err := os.Wait(pid, 0)
	if err != nil {
		sv.logger.Log(err)
		return false
	}

	sv.logger.Log(w)

	if (w.Exited() && w.ExitStatus() > 0) || w.Signaled() {
		go sv.c.Set(sv.prefix+runKey+"/"+sv.name, w.String(), store.Clobber)
		return false
	}
	return true
}

func (sv *service) run() {
	for done := false; !done; _, done = <-sv.done {
		ok := sv.once()
		if !ok {
			sv.logger.Log("fatal error -- giving up")
			return
		}
	}
}

func (sv *service) stop() {
	close(sv.done)
}

type monitor struct {
	self, prefix string
	st   *store.Store
	c    *client.Client
}

func Monitor(self, prefix string, st *store.Store) os.Error {
	logger := util.NewLogger("monitor")
	v, cas := st.Lookup("/j/junta/leader")
	if cas == store.Dir || cas == store.Missing {
		return os.NewError("boo") // TODO do this right
	}
	leader := v[0]

	v, cas = st.Lookup("/j/junta/members/"+leader)
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
	st.Watch(serviceKey + "/*", evs)
	names, cas := st.Lookup(serviceKey)
	if cas == store.Dir {
		for _, name := range names {
			path := serviceKey + "/" + name
			v, cas := st.Lookup(path)
			if cas != store.Dir && cas != store.Missing {
				logger.Log("injecting", name)
				go func(e store.Event) {
					evs <- e
				}(store.Event{0, path, v[0], cas, "", nil})
			}
		}
	}

	services := make(map[string]*service)
	for ev := range evs {
		_, name := path.Split(ev.Path)
		s, ok := services[name]
		switch {
		case ev.IsSet() && !ok:
			logger.Log("creating", name)
			s = newService(name, ev.Body, mon)
		case ev.IsDel() && ok:
			logger.Log("stopping", name)
			s.stop()
		}
		services[name] = s, ev.IsSet()
	}
	panic("not reached")
}
