package mon

import (
	"junta/client"
	"junta/store"
	"junta/util"
	"log"
	"os"
	"syscall"
	"strings"
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
