package store

import (
	"container/list"
	"gob"
	"io"
	"junta/util"
	"math"
	"os"
	"regexp"
	"strings"
	"strconv"
)

type Event struct {
	Seqn  uint64
	Path  string
	Body  string

	// the cas token for the key/value pair as of this event.
	// if the operation set a value, `Cas` is `Seqn` in decimal.
	// if the operation deleted a value, `Cas` is `Missing`.
	Cas   string

	// the mutation that caused this event
	Mut   string

	Err   os.Error
}

const (
	Clobber = ""
	Missing = "0"
	Dir = "dir"
)

var (
	BadPathError = os.NewError("bad path")
	BadMutationError = os.NewError("bad mutation")
	TooLateError = os.NewError("too late")
	CasMismatchError = os.NewError("cas mismatch")
)

var waitRegexp = regexp.MustCompile(``)

// This structure should be kept immutable.
type node struct {
	v string
	cas string
	ds map[string]node
}

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
	v string
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

func (n node) readdir() string {
	names := make([]string, len(n.ds))
	i := 0
	for name, _ := range n.ds {
		names[i] = name + "\n"
		i++
	}
	return strings.Join(names, "")
}

func (n node) get(parts []string) (string, string) {
	switch len(parts) {
	case 0:
		if len(n.ds) > 0 {
			return n.readdir(), n.cas
		} else {
			return n.v, n.cas
		}
	default:
		if m, ok := n.ds[parts[0]]; ok {
			return m.get(parts[1:])
		}
		return "", Missing
	}
	panic("can't happen")
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

func (n node) getp(path string) (string, string) {
	if err := checkPath(path); err != nil {
		return "", Missing
	}

	return n.get(split(path))
}

func copyMap(a map[string]node) map[string]node {
	b := make(map[string]node)
	for k,v := range a {
		b[k] = v
	}
	return b
}

// Return value is replacement node
func (n node) set(parts []string, v, cas string, keep bool) (node, bool) {
	if len(parts) == 0 {
		return node{v, cas, n.ds}, keep
	}

	n.ds = copyMap(n.ds)
	p, ok := n.ds[parts[0]].set(parts[1:], v, cas, keep)
	n.ds[parts[0]] = p, ok
	n.cas = Dir
	return n, len(n.ds) > 0
}

func (n node) setp(k, v, cas string, keep bool) node {
	if err := checkPath(k); err != nil {
		return n
	}

	n, _ = n.set(split(k), v, cas, keep)
	return n
}

func checkPath(k string) os.Error {
	switch {
	case len(k) < 1,
	     k[0] != '/',
	     len(k) > 1 && k[len(k) - 1] == '/',
	     strings.Count(k, "=") > 0,
	     strings.Count(k, " ") > 0:
		return BadPathError
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

func decode(mutation string) (path, v, cas string, keep bool, err os.Error) {
	cm := strings.Split(mutation, ":", 2)

	if len(cm) != 2 {
		err = BadMutationError
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
func wbuffer(in, out chan Event) {
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
	values := node{v:"", ds:make(map[string]node), cas:Dir}

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
				w.in <- Event{Seqn:w.stop, Err:TooLateError}
				close(w.in)
			}
		}

		// If we have any mutations that can be applied, do them.
		for t, ok := s.todo[ver+1]; ok; t, ok = s.todo[ver+1] {
			var nver uint64
			var err os.Error

			d := gob.NewDecoder(strings.NewReader(t.mutation))
			if t.seqn == 1 && d.Decode(&nver) == nil {
				var vx node
				err := d.Decode(&vx)
				if err == nil {
					values = vx
					for i := ver+1; i <= nver; i++ {
						s.todo[i] = apply{}, false
					}
					ver = nver
				} else {
					ver++
				}
			} else {
				var keep bool
				var k, v, givenCas, cas string
				k, v, givenCas, keep, err = decode(t.mutation)
				if err == nil {
					_, curCas := values.getp(k)
					if curCas == givenCas || givenCas == Clobber {
						cas = strconv.Uitoa64(t.seqn)
						if !keep {
							cas = Missing
						}
						values = values.setp(k, v, cas, keep)
						logger.Logf("store applied %v", t)
					} else {
						err = CasMismatchError
						k = "/store/error"
					}
				} else {
					k = "/store/error"
				}
				s.notify(Event{t.seqn, k, v, cas, t.mutation, err})
				s.todo[ver+1] = apply{}, false
				ver++
			}
		}
	}
}

// Applies `mutation` in sequence at position `seqn`. A malformed mutation is
// treated as a no-op. If a mutation has already been applied at this position,
// this one is sliently ignored.
//
// If `mutation` is a snapshot, notifications will not be sent.
func (s *Store) Apply(seqn uint64, mutation string) {
	s.applyCh <- apply{seqn, mutation}
}

// Gets the value stored at `path`, if any. If no value is stored at `path`,
// `ok` is false.
func (s *Store) Lookup(path string) (body string, cas string) {
	l := lookup{k:path, ch:make(chan int)}
	s.lookupCh <- &l
	<-l.ch
	return l.v, l.cas
}

// Encodes the entire storage state, including the current sequence number, as
// a mutation, and writes the mutation to `w`. This mutation can be applied to
// an empty store to reproduce the state of this one.
//
// A snapshot must be applied at sequence number `1`. Once a snapshot has been
// applied, the store's current sequence number will be set to that of the
// snapshot.
//
// Note that applying a snapshot does not send notifications.
func (s *Store) Snapshot(w io.Writer) (err os.Error) {
	ch := make(chan snap)
	s.snapCh <- ch
	ss := <-ch
	err = gob.NewEncoder(w).Encode(ss.ver)
	if err != nil {
		return
	}

	err = gob.NewEncoder(w).Encode(ss.root)
	return
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
	go wbuffer(in, ch)
	s.watchCh <- watch{pat:pattern, out:ch, in:in, re:re, stop:math.MaxUint64}
}

// Waits for `seqn` and sends a single event representing the change made at
// that position.
//
// If `seqn` is in the past, the event's `Err` will be `TooLateError`.
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

func (st *Store) Sync(seqn uint64) {
	ch := make(chan Event)
	st.Wait(seqn, ch)
	<-ch
}
