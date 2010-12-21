package store

import (
	"bytes"
	"gob"
	"doozer/util"
	"math"
	"os"
	"regexp"
	"strings"
)

const (
	Clobber = ""
	Missing = "0"
	Dir     = "dir"
)

// TODO revisit this when package regexp is more complete (e.g. do Unicode)
const (
	charPat = `([a-zA-Z0-9.]|-)`
	partPat = "/" + charPat + "+"
	pathPat = "^/$|^(" + partPat + ")+$"
)

var pathRe = regexp.MustCompile(pathPat)

var Any = MustCompileGlob("**")

var (
	ErrBadMutation = os.NewError("bad mutation")
	ErrBadSnapshot = os.NewError("bad snapshot")
	ErrTooLate     = os.NewError("too late")
	ErrCasMismatch = os.NewError("cas mismatch")
)

type BadPathError struct {
	Path string
}

func (e *BadPathError) String() string {
	return "bad path: " + e.Path
}

// Applies mutations sent on Ops in sequence according to field Seqn. Any
// errors that occur will be written to ErrorPath. Duplicate operations at a
// given position are sliently ignored.
type Store struct {
	Ops     chan<- Op
	Seqns   <-chan uint64
	Watches <-chan int
	watchCh chan *Watch
	watches []*Watch
	todo    map[uint64]Op
	state   *state
	log     map[uint64]Event
	cleanCh chan uint64
	notices []notice
}

// Represents an operation to apply to the store at position Seqn.
//
// If Mut is a snapshot, notifications will not be sent.
//
// If Mut is Nop, no change will be made, but a dummy event will still be sent.
type Op struct {
	Seqn uint64
	Mut  string
}

type state struct {
	ver  uint64
	root node
}

type Watch struct {
	C        <-chan Event
	c        chan<- Event
	glob     *Glob
	from, to uint64
	shutdown chan bool
}

func (wt *Watch) Stop() {
	_ = wt.shutdown <- true
}

type notice struct {
	ch chan<- Event
	ev Event
}

// Creates a new, empty data store. Mutations will be applied in order,
// starting at number 1 (number 0 can be thought of as the creation of the
// store).
func New() *Store {
	ops := make(chan Op)
	seqns := make(chan uint64)
	watches := make(chan int)

	st := &Store{
		Ops:     ops,
		Seqns:   seqns,
		Watches: watches,
		watchCh: make(chan *Watch),
		todo:    make(map[uint64]Op),
		watches: []*Watch{},
		state:   &state{0, emptyDir},
		log:     make(map[uint64]Event),
		cleanCh: make(chan uint64),
	}

	go st.process(ops, seqns, watches)
	return st
}

func split(path string) []string {
	if path == "/" {
		return []string{}
	}
	return strings.Split(path[1:], "/", -1)
}

func join(parts []string) string {
	return "/" + strings.Join(parts, "/")
}

func checkPath(k string) os.Error {
	if !pathRe.MatchString(k) {
		return &BadPathError{k}
	}
	return nil
}

// Returns a mutation that can be applied to a `Store`. The mutation will set
// the contents of the file at `path` to `body` iff the CAS token of that file
// matches `cas` at the time of application.
//
// If `path` is not valid, returns a `BadPathError`.
func EncodeSet(path, body string, cas string) (mutation string, err os.Error) {
	if err = checkPath(path); err != nil {
		return
	}
	return cas + ":" + path + "=" + body, nil
}

// Returns a mutation that can be applied to a `Store`. The mutation will cause
// the file at `path` to be deleted iff the CAS token of that file matches
// `cas` at the time of application.
//
// If `path` is not valid, returns a `BadPathError`.
func EncodeDel(path string, cas string) (mutation string, err os.Error) {
	if err := checkPath(path); err != nil {
		return
	}
	return cas + ":" + path, nil
}

// MustEncodeSet is like EncodeSet but panics if the mutation cannot be
// encoded. It simplifies safe initialization of global variables holding
// mutations.
func MustEncodeSet(path, body, cas string) (mutation string) {
	m, err := EncodeSet(path, body, cas)
	if err != nil {
		panic(err)
	}
	return m
}

// MustEncodeDel is like EncodeDel but panics if the mutation cannot be
// encoded. It simplifies safe initialization of global variables holding
// mutations.
func MustEncodeDel(path, cas string) (mutation string) {
	m, err := EncodeDel(path, cas)
	if err != nil {
		panic(err)
	}
	return m
}

func decode(mutation string) (path, v, cas string, keep bool, err os.Error) {
	cm := strings.Split(mutation, ":", 2)

	if len(cm) != 2 {
		err = ErrBadMutation
		return
	}

	kv := strings.Split(cm[1], "=", 2)

	if err = checkPath(kv[0]); err != nil {
		return
	}

	switch len(kv) {
	case 1:
		return kv[0], "", cm[0], false, nil
	case 2:
		return kv[0], kv[1], cm[0], true, nil
	}
	panic("unreachable")
}

func (st *Store) notify(e Event, ws []*Watch) []*Watch {
	nwatches := make([]*Watch, len(ws))

	i := 0
	for _, w := range ws {
		if _, ok := <-w.shutdown; ok {
			continue
		}

		if e.Seqn >= w.to {
			continue
		}

		last := w.to - 1
		if e.Seqn != last {
			nwatches[i] = w
			i++
		}

		if e.Seqn < w.from {
			continue
		}

		if w.glob.Match(e.Path) {
			st.notices = append(st.notices, notice{w.c, e})
		}
	}

	return nwatches[0:i]
}

func (st *Store) closeWatches() {
	for _, w := range st.watches {
		close(w.c)
	}
}

func (st *Store) process(ops <-chan Op, seqns chan<- uint64, watches chan<- int) {
	logger := util.NewLogger("store")
	defer st.closeWatches()

	var head uint64

	for {
		ver, values := st.state.ver, st.state.root

		for len(st.notices) > 0 && closed(st.notices[0].ch) {
			st.notices = st.notices[1:]
		}

		var n notice
		if len(st.notices) > 0 {
			n = st.notices[0]
		}

		// Take any incoming requests and queue them up.
		select {
		case a := <-ops:
			if closed(ops) {
				return
			}

			if a.Seqn > ver {
				st.todo[a.Seqn] = a
			}
		case w := <-st.watchCh:
			n, ws := w.from, []*Watch{w}
			for ; len(ws) > 0 && n < head; n++ {
				ws = st.notify(Event{Seqn: n, Err: ErrTooLate}, ws)
			}
			for ; len(ws) > 0 && n <= ver; n++ {
				ws = st.notify(st.log[n], ws)
			}

			st.watches = append(st.watches, ws...)
		case seqn := <-st.cleanCh:
			for ; head <= seqn; head++ {
				st.log[head] = Event{}, false
			}
		case seqns <- ver:
			// nothing to do here
		case watches <- len(st.watches):
			// nothing to do here
		case n.ch <- n.ev:
			st.notices = st.notices[1:]
		}

		// If we have any mutations that can be applied, do them.
		for t, ok := st.todo[ver+1]; ok; t, ok = st.todo[ver+1] {
			var ev Event
			var snap bool
			values, ev, snap = values.apply(t.Seqn, t.Mut)
			logger.Printf("apply %s %v %v %v %v %v", ev.Desc(), ev.Seqn, ev.Path, ev.Body, ev.Cas, ev.Err)
			st.state = &state{ev.Seqn, values}
			st.log[t.Seqn] = ev
			st.watches = st.notify(ev, st.watches)
			for ver < ev.Seqn {
				ver++
				st.todo[ver] = Op{}, false
			}
			if snap {
				head = ev.Seqn + 1
			}
		}
	}
}

// Returns a point-in-time snapshot of the contents of the store.
func (st *Store) Snap() (ver uint64, g Getter) {
	// WARNING: Be sure to read the pointer value of st.state only once. If you
	// need multiple accesses, copy the pointer first.
	p := st.state

	return p.ver, p.root
}

// Gets the value stored at `path`, if any.
//
// If no value is stored at `path`, `cas` will be `Missing` and `value` will be
// nil.
//
// if `path` is a directory, `cas` will be `Dir` and `value` will be a list of
// entries.
//
// Otherwise, `cas` is the CAS token and `value[0]` is the body.
func (st *Store) Get(path string) (value []string, cas string) {
	_, g := st.Snap()
	return g.Get(path)
}

// Encodes the entire storage state, including the current sequence number, as
// a mutation. This mutation can be applied to an empty store to reproduce the
// state of `s`.
//
// Returns the sequence number of the snapshot and the mutation itself.
//
// A snapshot must be applied at sequence number 1. Once a snapshot has been
// applied, the store's sequence number will be set to `seqn`.
//
// Note that applying a snapshot does not send notifications.
func (st *Store) Snapshot() (seqn uint64, mutation string) {
	w := new(bytes.Buffer)
	ver, g := st.Snap()

	err := gob.NewEncoder(w).Encode(ver)
	if err != nil {
		panic(err)
	}

	err = gob.NewEncoder(w).Encode(g)
	if err != nil {
		panic(err)
	}

	return ver, w.String()
}

// A convenience wrapper for NewWatch that returns only the channel. Useful for
// code that never needs to stop the Watch.
func (st *Store) Watch(glob *Glob) <-chan Event {
	return NewWatch(st, glob).C
}

// Arranges for w to receive notifications when mutations are applied to st.
// One event e will be received from w.C for each mutation iff
// glob.Match(e.Path).
//
// Notifications will not be sent for changes made as the result of applying a
// snapshot.
func NewWatch(st *Store, glob *Glob) (w *Watch) {
	ch := make(chan Event)
	ver, _ := st.Snap()
	return st.watchOn(glob, ch, ver+1, math.MaxUint64)
}

func (st *Store) watchOn(glob *Glob, ch chan Event, from, to uint64) *Watch {
	wt := &Watch{
		C:        ch,
		c:        ch,
		glob:     glob,
		from:     from,
		to:       to,
		shutdown: make(chan bool, 1),
	}
	st.watchCh <- wt
	return wt
}

// Returns a read-only chan that will receive a single event representing the
// change made at position `seqn`.
//
// If `seqn` was applied before the call to `Wait`, a dummy event will be
// sent with its `Err` set to `ErrTooLate`.
func (st *Store) Wait(seqn uint64) <-chan Event {
	ch := make(chan Event, 1)
	st.watchOn(Any, ch, seqn, seqn+1)
	return ch
}

// Ensures that the application of mutation at `seqn` happens before the call
// to `Sync` returns.
//
// See http://golang.org/doc/go_mem.html for the meaning of "happens before" in
// Go.
func (st *Store) Sync(seqn uint64) {
	<-st.Wait(seqn)
}

// Returns an immutable copy of `st` in which `path` exists as a regular file
// (not a dir). Waits for `path` to be set, if necessary.
func (st *Store) SyncPath(path string) (Getter, os.Error) {
	glob, err := CompileGlob(path)
	if err != nil {
		return nil, err
	}

	wt := NewWatch(st, glob)
	defer wt.Stop()

	_, g := st.Snap()
	_, cas := g.Get(path)
	if cas != Dir && cas != Missing {
		return g, nil
	}

	for ev := range wt.C {
		if ev.IsSet() {
			return ev, nil
		}
	}

	panic("unreachable")
}

func (st *Store) Clean(seqn uint64) {
	st.cleanCh <- seqn
}
