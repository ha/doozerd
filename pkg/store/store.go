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
	lookupCh chan *lookup
	snapCh chan chan snap
	watchCh chan watch
	watches []watch
	todo map[uint64]apply
}

type apply struct {
	seqn uint64
	mutation string
}

type lookup struct {
	k string
	ch chan int
	v []string
	cas string
}

type snap struct {
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
		lookupCh: make(chan *lookup),
		snapCh: make(chan chan snap),
		watchCh: make(chan watch),
		todo: make(map[uint64]apply),
		watches: []watch{},
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

func EncodeSet(path, body string, cas string) (mutation string, err os.Error) {
	if err = checkPath(path); err != nil {
		return
	}
	return cas + ":" + path + "=" + body, nil
}

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
	panic("can't happen")
}

func (s *Store) notify(e Event) {
	for _, w := range s.watches {
		if w.re.MatchString(e.Path) {
			w.in <- e
		}
	}
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
			list.Remove(f)
		}
	}
}

func (s *Store) process() {
	logger := util.NewLogger("store")

	ver := uint64(0)
	values := root

	for {
		// Take any incoming requests and queue them up.
		select {
		case a := <-s.applyCh:
			if a.seqn > ver {
				s.todo[a.seqn] = a
			}
		case r := <-s.lookupCh:
			r.v, r.cas = values.getp(r.k)
			r.ch <- 1
		case ch := <-s.snapCh:
			ch <- snap{ver, values}
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
// Otherwise, `cas` is the cas token and `value[0]` is the body.
func (s *Store) Lookup(path string) (value []string, cas string) {
	l := lookup{k:path, ch:make(chan int)}
	s.lookupCh <- &l
	<-l.ch
	return l.v, l.cas
}

// Retrieves the body stored at `path` and returns it. If `path` is a directory
// or does not exist, returns an empty string.
//
// Note, with this function it is impossible to distinguish between an empty
// string stored at `path`, a missing entry, and a directory. If you need to
// tell the difference, use `Lookup`.
//
// Also note, this function does not return the CAS token for `path`. If you
// need the CAS token, use `Lookup`.
func (s *Store) LookupString(path string) (body string) {
	v, cas := s.Lookup(path)
	if cas == Missing || cas == Dir {
		return ""
	}
	return v[0]
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
	ch := make(chan snap)
	s.snapCh <- ch
	ss := <-ch
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
//  - `?` matches a single char in a single path component
//  - `*` matches zero or more chars in a single path component
//  - `**` matches zero or more chars in zero or more components
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
				ch <- e
			}
		}
	}()
}

// Ensures that the application of mutation at `seqn` happens before the call
// to `Sync` returns.
func (st *Store) Sync(seqn uint64) {
	ch := make(chan Event)
	st.Wait(seqn, ch)
	<-ch
}
