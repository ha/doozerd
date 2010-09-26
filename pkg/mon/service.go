package mon

import (
	"junta/store"
	"junta/util"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const (
	never = iota
	restartOnSuccess
	restartAlways
)

var restart = map[string]int{
	"":                   never,
	"never":              never,
	"restart-on-success": restartOnSuccess,
	"restart-always":     restartAlways,
}

type service struct {
	id, name     string
	pid          int
	st           *store.Store
	self, prefix string
	cl           SetDeler
	logger       *log.Logger
	mon          *monitor
	wantUp       bool
	lockCas      string
	lockTaken    bool
	restart      int
}

func newService(id, name string, mon *monitor) *service {
	sv := &service{
		id:     id,
		name:   name,
		st:     mon.st,
		self:   mon.self,
		cl:     mon.cl,
		mon:    mon,
		logger: util.NewLogger(id),
		prefix: mon.prefix,
	}
	sv.logger.Log("new")
	return sv
}

func (sv *service) StartWithFds(fds []int) {
	panic("implement me")
}

func (sv *service) tryLock() {
	sv.mon.tryLock(sv.id)
}

func (sv *service) release() {
	if sv.lockCas == "" {
		return
	}

	go sv.mon.release(sv.id, sv.lockCas)
}

func (sv *service) lookupRestart() (int, os.Error) {
	r, ok := restart[sv.lookupParam("service/restart")]
	if !ok {
		return 0, os.NewError("bah")
	}
	return r, nil
}

func (sv *service) exec() {
	if sv.pid != 0 {
		return
	}

	var err os.Error

	sv.logger.Log("exec")
	cmd := sv.lookupParam("service/exec-start")
	args := strings.Split(cmd, " ", -1)

	sv.restart, err = sv.lookupRestart()
	if err != nil {
		goto error
	}

	sv.logger.Log("*** *** *** RUN *** *** ***")
	sv.pid, err = os.ForkExec(args[0], args, nil, "", nil)
	if err != nil {
		goto error
	}

	go sv.setStatus("status", "up")
	go sv.delStatus("reason")
	go sv.setStatus("pid", strconv.Itoa(sv.pid))
	go sv.mon.wait(sv.pid, sv)
	return

error:
	sv.wantUp = false // fatal error -- don't retry
	sv.logger.Log(err)
	go sv.setStatus("status", "down")
	go sv.setStatus("reason", err.String())
}

func (sv *service) kill() {
	if sv.pid == 0 {
		return
	}

	errno := syscall.Kill(sv.pid, syscall.SIGTERM)
	if errno != 0 {
		sv.logger.Log(os.Errno(errno))
	}
}

func (sv *service) lookupParam(param string) string {
	return sv.mon.lookupParam(sv.id, param)
}

func (sv *service) setStatus(param, val string) {
	sv.mon.setStatus(sv.id, param, val)
}

func (sv *service) delStatus(param string) {
	sv.mon.delStatus(sv.id, param)
}

func (sv *service) isFatal(w *os.Waitmsg) bool {
	switch sv.restart {
	case restartOnSuccess:
		return !exitedCleanly(w)
	case restartAlways:
		return false
	}
	return true
}

func (sv *service) exited(w *os.Waitmsg) {
	if w.Pid != sv.pid {
		return
	}
	sv.pid = 0

	sv.logger.Log(w)
	go sv.delStatus("pid")

	if sv.isFatal(w) {
		sv.wantUp = false
		sv.logger.Log("fatal error")
		go sv.setStatus("status", "down")
		go sv.setStatus("reason", w.String())
	} else {
		go sv.setStatus("status", "restarting")
		go sv.setStatus("reason", w.String())
	}

	sv.check()
}

func (sv *service) check() {
	sv.logger.Log("checking up/down state")

	if sv.wantUp {
		if sv.lockCas == "" {
			sv.kill()
			if !sv.lockTaken {
				go sv.tryLock()
			}
		} else {
			sv.exec()
		}
	} else {
		if sv.pid != 0 {
			sv.kill()
			go sv.mon.timer(sv, 1000)
		} else {
			sv.release()
		}
	}
}

func (sv *service) start() {
	sv.logger.Log("starting")
	sv.wantUp = true
	sv.check()
}

func (sv *service) stop() {
	sv.logger.Log("stopping")
	sv.wantUp = false
	sv.check()
}

func (sv *service) tick() {
	sv.check()
}

func (sv *service) dispatchLockEvent(ev store.Event) {
	sv.logger.Log("got lock event", ev)
	if ev.Body == sv.self {
		sv.lockCas, sv.lockTaken = ev.Cas, true
		go sv.setStatus("node", sv.self)
		go sv.setStatus("host", sv.mon.host)
	} else {
		sv.lockCas, sv.lockTaken = "", ev.Body != ""
	}
	sv.check()
}
