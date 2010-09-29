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
	lis          []net.Listener
	lfiles       []*os.File
	mon          *monitor
	wantUp       bool
	lockCas      string
	lockTaken    bool
	restart      int
}

type filer interface {
	File() *os.File
}

func newSocket(id, name string, mon *monitor) *socket {
	sv := mon.increfService(name+".service")
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
	so.logger.Log("new")
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

	so.logger.Log("open")

	laddr := so.lookupParam("socket/listen-stream")
	li, err := net.Listen("tcp", laddr)
	if err != nil {
		goto error
	}

	f, ok := li.(filer)
	if !ok {
		goto error // can't happen
	}

	so.lis = []net.Listener{li}
	so.lfiles = []*os.File{f.File()}
	so.sv.SetFiles(so.lfiles)

	go so.setStatus("status", "up")
	go so.delStatus("reason")
	go so.setStatus("listen-addr", li.Addr().String())
	go so.mon.poll(so.lfiles, so)
	return

error:
	so.wantUp = false // fatal error -- don't retry
	so.logger.Log(err)
	go so.setStatus("status", "down")
	go so.setStatus("reason", err.String())
}

func (so *socket) close() {
	if so.lfiles == nil {
		return
	}

	for _, f := range so.lfiles {
		f.Close()
	}
	so.lfiles = nil

	so.sv.stop()
}

func (so *socket) ready(f *os.File) {
	so.sv.start()
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
	so.logger.Log("checking up/down state")

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
	so.logger.Log("starting")
	so.wantUp = true
	so.check()
}

func (so *socket) stop() {
	so.logger.Log("stopping")
	so.wantUp = false
	so.check()
}

func (so *socket) tick() {
}

func (so *socket) dispatchLockEvent(ev store.Event) {
	so.logger.Log("got lock event", ev)
	if ev.Body == so.self {
		so.lockCas, so.lockTaken = ev.Cas, true
		go so.setStatus("node", so.self)
	} else {
		so.lockCas, so.lockTaken = "", ev.Body != ""
	}
	so.check()
}
