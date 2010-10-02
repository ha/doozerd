package store

import (
	"bytes"
	"container/list"
	"gob"
	"junta/util"
	"math"
	"os"
	"regexp"
	"strings"
)

const (
	Clobber = ""
	Missing = "0"
	Dir = "dir"
)

var (
	ErrBadPath = os.NewError("bad path")
	ErrBadMutation = os.NewError("bad mutation")
	ErrBadSnapshot = os.NewError("bad snapshot")
	ErrTooLate = os.NewError("too late")
	ErrCasMismatch = os.NewError("cas mismatch")
)

var waitRegexp = regexp.MustCompile(``)

type Store struct {
	applyCh chan apply
	watchCh chan watch
	watches []watch
	todo map[uint64]apply
	state *state
}

type apply struct {
	seqn uint64
	mutation string
}

type state struct {
	ver uint64
	root node
}

type watch struct {
	pat string
	in, out chan Event
	re *regexp.Regexp
	stop uint64
}

// Creates a new, empty data store. Mutations will be applied in order,
// starting at number 1 (number 0 can be thought of as the creation of the
// store).
func New() *Store {
	s := &Store{
		applyCh: make(chan apply),
		watchCh: make(chan watch),
		todo: make(map[uint64]apply),
		watches: []watch{},
		state: &state{0, emptyDir},
	}

	go s.process()
	return s
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
	switch {
	case len(k) < 1,
	     k[0] != '/',
	     len(k) > 1 && k[len(k) - 1] == '/',
	     strings.Count(k, "=") > 0,
	     strings.Count(k, " ") > 0:
		return ErrBadPath
	}
	return nil
}

// Returns a mutation that can be applied to a `Store`. The mutation will set
// the contents of the file at `path` to `body` iff the CAS token of that file
// matches `cas` at the time of application.
//
// If `path` is not valid, returns `ErrBadPath`.
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
// If `path` is not valid, returns `ErrBadPath`.
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

func (s *Store) notify(e Event) {
	nwatches := make([]watch, len(s.watches))

	i := 0
	for _, w := range s.watches {
		if closed(w.in) {
			continue
		}

		nwatches[i] = w
		i++

		if w.re.MatchString(e.Path) {
			w.in <- e
		}
	}

	s.watches = nwatches[0:i]
}

func append(ws *[]watch, w watch) {
	l := len(*ws)
	if l + 1 > cap(*ws) {
		ns := make([]watch, (l + 1)*2)
		copy(ns, *ws)
		*ws = ns
	}
	*ws = (*ws)[0:l + 1]
	(*ws)[l] = w
}

// Unbounded in-order buffering
func buffer(in, out chan Event) {
	list := list.New()
	for {
		f, e := list.Front(), Event{}
		var ch chan Event
		if f != nil {
			e = f.Value.(Event)
			ch = out
		}
		select {
		case x := <-in:
			if closed(in) {
				return
			}
			list.PushBack(x)
		case ch <- e:
			if closed(ch) {
				close(in)
				<-in
				return
			}
			list.Remove(f)
		}
	}
}

func (s *Store) process() {
	logger := util.NewLogger("store")

	for {
		ver, values := s.state.ver, s.state.root

		// Take any incoming requests and queue them up.
		select {
		case a := <-s.applyCh:
			if a.seqn > ver {
				s.todo[a.seqn] = a
			}
		case w := <-s.watchCh:
			if w.stop > ver {
				append(&s.watches, w)
			} else {
				w.in <- Event{Seqn:w.stop, Err:ErrTooLate}
				close(w.in)
			}
		}

		// If we have any mutations that can be applied, do them.
		for t, ok := s.todo[ver+1]; ok; t, ok = s.todo[ver+1] {
			var ev Event
			values, ev = values.apply(t.seqn, t.mutation)
			logger.Logf("apply %s %v %v %v %v %v", ev.Desc(), ev.Seqn, ev.Path, ev.Body, ev.Cas, ev.Err)
			s.state = &state{ev.Seqn, values}
			s.notify(ev)
			for ver < ev.Seqn {
				ver++
				s.todo[ver] = apply{}, false
			}
		}
	}
}

// Applies `mutation` in sequence at position `seqn`. Any error that occurs
// will be written to `ErrorPath`. If a mutation has already been applied at
// this position, this one is sliently ignored.
//
// If `mutation` is a snapshot, notifications will not be sent.
//
// If `mutation` is `Nop`, no change will be made, but a dummy event will still
// be sent.
func (s *Store) Apply(seqn uint64, mutation string) {
	s.applyCh <- apply{seqn, mutation}
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
func (s *Store) Get(path string) (value []string, cas string) {
	// WARNING: Be sure to read the pointer value of s.state only once. If you
	// need multiple accesses, copy the pointer first.
	return s.state.root.Get(path)
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
func (s *Store) Snapshot() (seqn uint64, mutation string) {
	w := new(bytes.Buffer)

	// WARNING: Be sure to read the pointer value of s.state only once. If you
	// need multiple accesses, copy the pointer first.
	ss := s.state

	err := gob.NewEncoder(w).Encode(ss.ver)
	if err != nil {
		panic(err)
	}

	err = gob.NewEncoder(w).Encode(ss.root)
	if err != nil {
		panic(err)
	}

	return ss.ver, w.String()
}

// Subscribes `ch` to receive notifications when mutations are applied to paths
// in the store. One event will be sent for each mutation iff the event's path
// matches `pattern`, a Unix-style glob pattern.
//
// Glob notation:
//  - "?" matches a single char in a single path component
//  - "*" matches zero or more chars in a single path component
//  - "**" matches zero or more chars in zero or more components
//  - any other sequence matches itself
//
// Notifications will not be sent for changes made as the result of applying a
// snapshot.
func (s *Store) Watch(pattern string, ch chan Event) {
	re, _ := compileGlob(pattern)
	in := make(chan Event)
	go buffer(in, ch)
	s.watchCh <- watch{pat:pattern, out:ch, in:in, re:re, stop:math.MaxUint64}
}

// Subscribes `ch` to receive a single event representing the change made at
// position `seqn`.
//
// If `seqn` was applied before the call to `Wait`, a dummy event will be
// sent with its `Err` set to `ErrTooLate`.
func (s *Store) Wait(seqn uint64, ch chan Event) {
	all := make(chan Event)
	s.watchCh <- watch{in:all, re:waitRegexp, stop:seqn}
	go func() {
		for e := range all {
			if e.Seqn == seqn {
				close(all)
				<-all
				ch <- e
			}
		}
	}()
}

// Ensures that the application of mutation at `seqn` happens before the call
// to `Sync` returns.
//
// See http://golang.org/doc/go_mem.html for the meaning of "happens before" in
// Go.
func (st *Store) Sync(seqn uint64) {
	ch := make(chan Event)
	st.Wait(seqn, ch)
	<-ch
}

// Returns an immutable copy of `st` in which `path` exists as a regular file
// (not a dir). Waits for `path` to be set, if necessary.
func (st *Store) SyncPath(path string) Getter {
	evs := make(chan Event)
	defer close(evs)

	st.Watch(path, evs)

	g := st.state.root // TODO make this use a public method
	_, cas := g.Get(path)
	if cas != Dir && cas != Missing {
		return g
	}

	for ev := range evs {
		if ev.IsSet() {
			return ev
		}
	}

	panic("unreachable")
}
