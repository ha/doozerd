package mon

import (
	"junta/store"
	"junta/util"
	"log"
	"net"
	"os"
)

type socket struct {
	id, name     string
	sv           *service
	st           *store.Store
	self, prefix string
	cl           SetDeler
	logger       *log.Logger
	lfiles       []*os.File
	mon          *monitor
	wantUp       bool
	lockCas      string
	lockTaken    bool
	restart      int
}

type filer interface {
	File() (*os.File, os.Error)
}

func newSocket(id, name string, mon *monitor) *socket {
	sv := mon.increfService(name + ".service")
	if sv == nil {
		return nil
	}

	so := &socket{
		id:     id,
		name:   name,
		st:     mon.st,
		self:   mon.self,
		cl:     mon.cl,
		sv:     sv,
		mon:    mon,
		logger: util.NewLogger(id),
		prefix: mon.prefix,
	}
	so.logger.Println("new")
	return so
}

func (so *socket) tryLock() {
	so.mon.tryLock(so.id)
}

func (so *socket) release() {
	if so.lockCas == "" {
		return
	}

	go so.mon.release(so.id, so.lockCas)
}

func (so *socket) lookupRestart() (int, os.Error) {
	r, ok := restart[so.lookupParam("socket/restart")]
	if !ok {
		return 0, os.NewError("bah")
	}
	return r, nil
}

func (so *socket) open() {
	if so.lfiles != nil {
		return
	}

	so.logger.Println("open")

	laddr := so.lookupParam("socket/listen-stream")
	li, err := net.Listen("tcp", laddr)
	if err != nil {
		goto error
	}

	f, ok := li.(filer)
	if !ok {
		err = os.NewError("cannot convert li to filer")
		goto error // can't happen
	}

	var file *os.File
	file, err = f.File()
	if err != nil {
		goto error
	}

	so.lfiles = []*os.File{file}

	// Propagate our "want up" state to the sv. Note: this won't actualy run
	// the service yet, since sv.lfiles is still nil.
	so.sv.start() // just mark sv.wantUp = true

	go so.setStatus("status", "up")
	go so.delStatus("reason")
	go so.setStatus("listen-addr", li.Addr().String())
	go so.mon.poll(so.lfiles, so)

	// Tell the service about the socket so the service can notify the socket
	// when the process dies.
	so.sv.setSocket(so)

	return

error:
	so.wantUp = false // fatal error -- don't retry
	so.logger.Println(err)
	go so.setStatus("status", "down")
	go so.setStatus("reason", err.String())
	go so.delStatus("listen-addr")
}

// We want to know if the service quits or dies, so we can start it up
// again on socket activity.
func (so *socket) exited() {
	go so.mon.poll(so.lfiles, so)
}

func (so *socket) close() {
	if so.lfiles == nil {
		return
	}

	for _, f := range so.lfiles {
		f.Close()
	}
	so.lfiles = nil
	so.sv.setActiveLFDs(nil)
	so.logger.Println("closed, stopping service")
	so.sv.stop() // just mark sv.wantUp = false

	so.logger.Println("closed, updating status")
	go so.setStatus("status", "down")
	go so.setStatus("reason", "requested")
	go so.delStatus("listen-addr")
}

func (so *socket) ready(f *os.File) {
	so.sv.setActiveLFDs(so.lfiles)
}

func (so *socket) lookupParam(param string) string {
	return so.mon.lookupParam(so.id, param)
}

func (so *socket) setStatus(param, val string) {
	so.mon.setStatus(so.id, param, val)
}

func (so *socket) delStatus(param string) {
	so.mon.delStatus(so.id, param)
}

func (so *socket) isFatal(w *os.Waitmsg) bool {
	switch so.restart {
	case restartOnSuccess:
		return !exitedCleanly(w)
	case restartAlways:
		return false
	}
	return true
}

func (so *socket) check() {
	so.logger.Println("checking up/down state")

	if so.wantUp {
		if so.lockCas == "" {
			so.close()
			if !so.lockTaken {
				go so.tryLock()
			}
		} else {
			so.open()
		}
	} else {
		so.close()
		so.release()
	}
}

func (so *socket) start() {
	so.logger.Println("starting")
	so.wantUp = true
	so.check()
}

func (so *socket) stop() {
	so.logger.Println("stopping")
	so.wantUp = false
	so.check()
}

func (so *socket) tick() {
}

func (so *socket) dispatchLockEvent(ev store.Event) {
	so.logger.Println("got lock event", ev)
	if ev.Body == so.self {
		so.lockCas, so.lockTaken = ev.Cas, true
		go so.setStatus("node", so.self)
	} else {
		so.lockCas, so.lockTaken = "", ev.Body != ""
	}
	so.check()
}
