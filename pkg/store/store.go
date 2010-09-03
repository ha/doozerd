package store

import (
	"container/heap"
	"container/list"
	"container/vector"
	"gob"
	"io"
	"junta/util"
	"math"
	"os"
	"path"
	"strings"
)

type Event struct {
	Type uint
	Seqn uint64
	Path string
	Body string
}

const (
	Set = uint(1<<iota)
	Del
	Add
	Rem
	Apply
)

var conj = map[uint]uint{Set:Add, Del:Rem}

var (
	BadPathError = os.NewError("bad path")
	BadMutationError = os.NewError("bad mutation")
)

var LogWriter = util.NullWriter{}

// This structure should be kept immutable.
type node struct {
	v string
	ds map[string]*node
}

var emptyNode = node{v:"", ds:make(map[string]*node)}

type Store struct {
	applyCh chan apply
	reqCh chan req
	snapCh chan snap
	watchCh chan watch
	watches map[string][]watch
	notifyCh chan notify
	todo map[uint64]apply
	todoLookup *reqQueue
	todoSnap *snapQueue
}

type apply struct {
	seqn uint64
	mutation string
}

type req struct {
	k string
	seqn uint64
	ch chan reply
}

type snap struct {
	seqn uint64
	ch chan state
}

type state struct {
	ver uint64
	root node
}

type reqQueue struct {
	vector.Vector
}

type snapQueue struct {
	vector.Vector
}

type reply struct {
	v string
	ok bool
}

type watch struct {
	ch chan Event
	mask uint
	k string
	ready chan int
}

type notify struct {
	ch chan Event
	ev Event
}

func (r req) Less(y interface{}) bool {
	return r.seqn < y.(req).seqn
}

func (r snap) Less(y interface{}) bool {
	return r.seqn < y.(snap).seqn
}

func (q *reqQueue) peek() req {
	if len(q.Vector) == 0 {
		return req{seqn:math.MaxUint64} // ~infinity
	}
	return q.Vector[0].(req)
}

func (q *snapQueue) peek() snap {
	if len(q.Vector) == 0 {
		return snap{seqn:math.MaxUint64} // ~infinity
	}
	return q.Vector[0].(snap)
}

// Creates a new, empty data store. Mutations will be applied in order,
// starting at number 1 (number 0 can be thought of as the creation of the
// store).
func New() *Store {
	s := &Store{
		applyCh: make(chan apply),
		reqCh: make(chan req),
		snapCh: make(chan snap),
		watchCh: make(chan watch),
		todo: make(map[uint64]apply),
		todoLookup: new(reqQueue),
		todoSnap: new(snapQueue),
		watches: make(map[string][]watch),
		notifyCh: make(chan notify),
	}
	heap.Init(s.todoLookup)
	heap.Init(s.todoSnap)
	go s.buffer()
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

func (n node) get(parts []string) (string, bool) {
	switch len(parts) {
	case 0:
		if len(n.ds) > 0 {
			return n.readdir(), true
		} else {
			return n.v, true
		}
	default:
		if m, ok := n.ds[parts[0]]; ok {
			return m.get(parts[1:])
		}
		return "", false
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

func (n node) getp(path string) (string, bool) {
	if err := checkPath(path); err != nil {
		return "", false
	}

	return n.get(split(path))
}

// Return value:
//     y = replacement node
//     c = how many levels were changed (including the leaf)
func (n node) set(parts []string, v string, keep bool) (y *node, c int) {
	switch len(parts) {
	case 0:
		y = &node{v:v, ds:n.ds}
	default:
		d := 0
		m, ok := n.ds[parts[0]]
		if m == nil {
			m = &emptyNode
		}
		m, d = m.set(parts[1:], v, keep)
		ds := make(map[string]*node)
		for k,v := range n.ds {
			ds[k] = v
		}
		ds[parts[0]] = m, m != nil
		if ok != (m != nil) {
			c = 1
		}
		y = &node{v:n.v, ds:ds}
		c += d
	}
	if !keep && len(y.ds) == 0 {
		y = nil
	}
	return
}

func (n node) setp(k, v string, keep bool) (y node, ps []string) {
	if err := checkPath(k); err != nil {
		return n, []string{}
	}

	r, c := n.set(split(k), v, keep)
	ps = make([]string, c)
	for i := 0; i < c; i++ {
		ps[i] = k
		d, _ := path.Split(k)
		k = d[0:len(d) - 1]
	}

	if r == nil {
		return emptyNode, ps
	}
	return *r, ps
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
	return ":" + path, nil
}

func decode(mutation string) (op uint, path, v, cas string, err os.Error) {
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
		return Del, kv[0], "", "", nil
	case 2:
		return Set, kv[0], kv[1], cm[0], nil
	}
	panic("can't happen")
}

func (s *Store) notify(t uint, seqn uint64, k, v string) {
	for _, w := range s.watches[k] {
		if w.mask & t != 0 {
			s.notifyCh <- notify{w.ch, Event{t, seqn, k, v}}
		}
	}
}

func (s *Store) notifyDir(t uint, seqn uint64, p string) {
	dirname, basename := path.Split(p)
	if dirname != "/" {
		dirname = dirname[0:len(dirname) - 1] // strip slash
	}
	s.notify(t, seqn, dirname, basename)
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
func (s *Store) buffer() {
	list := list.New()
	var n notify
	for {
		select {
		case n.ch <- n.ev:
			n = notify{}
		case x := <-s.notifyCh:
			list.PushBack(x)
		}
		if x := list.Front(); x != nil && n.ch == nil {
			n = x.Value.(notify)
			list.Remove(x)
		}
	}
}

func (s *Store) process() {
	logger := util.NewLogger("store")

	ver := uint64(0)
	next := uint64(1)
	values := emptyNode
	for {
		// Take any incoming requests and queue them up.
		select {
		case a := <-s.applyCh:
			if a.seqn >= next {
				s.todo[a.seqn] = a
			}
		case r := <-s.reqCh:
			heap.Push(s.todoLookup, r)
		case r := <-s.snapCh:
			heap.Push(s.todoSnap, r)
		case w := <-s.watchCh:
			watches := s.watches[w.k]
			append(&watches, w)
			s.watches[w.k] = watches
			w.ready <- 1
		}

		// If we have any mutations that can be applied, do them.
		for t, ok := s.todo[next]; ok; t, ok = s.todo[next] {
			var nver uint64
			d := gob.NewDecoder(strings.NewReader(t.mutation))
			if t.seqn == 1 && d.Decode(&nver) == nil {
				var vx node
				err := d.Decode(&vx)
				if err == nil {
					values = vx
					next = nver
					s.todo = make(map[uint64]apply)
				}
			} else {
				op, k, v, _, err := decode(t.mutation)
				if err == nil {
					var changed []string
					values, changed = values.setp(k, v, op == Set)
					logger.Logf("store applied %v", t)
					if op == Set || len(changed) > 0 {
						s.notify(op, t.seqn, k, v)
					}
					for _, p := range changed {
						s.notifyDir(conj[op], t.seqn, p)
					}
					s.notify(Apply, t.seqn, "", "")
				}
			}
			ver = next
			s.todo[next] = apply{}, false
			next++
		}

		// If we have any lookups that can be satisfied, do them.
		for r := s.todoLookup.peek(); ver >= r.seqn; r = s.todoLookup.peek() {
			r := heap.Pop(s.todoLookup).(req)
			v, ok := values.getp(r.k)
			r.ch <- reply{v, ok}
		}

		// If we have any snapshots that can be satisfied, do them.
		for r := s.todoSnap.peek(); ver >= r.seqn; r = s.todoSnap.peek() {
			r := heap.Pop(s.todoSnap).(snap)
			r.ch <- state{ver, values}
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
func (s *Store) Lookup(path string) (body string, ok bool) {
	return s.LookupSync(path, 0)
}

// Like `Lookup`, but waits until after mutation number `seqn` has been
// applied before doing the lookup.
func (s *Store) LookupSync(path string, seqn uint64) (body string, ok bool) {
	ch := make(chan reply)
	s.reqCh <- req{path, seqn, ch}
	rep := <-ch
	return rep.v, rep.ok
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
	return s.SnapshotSync(0, w)
}

// Like `Snapshot`, but waits until after mutation number `seqn` has been
// applied before making the snapshot.
func (s *Store) SnapshotSync(seqn uint64, w io.Writer) (err os.Error) {
	ch := make(chan state)
	s.snapCh <- snap{seqn, ch}
	st := <-ch
	err = gob.NewEncoder(w).Encode(st.ver)
	if err != nil {
		return
	}

	err = gob.NewEncoder(w).Encode(st.root)
	return
}

// Subscribes `events` to receive notifications when mutations are applied to
// a path in the store. Set `mask` to one or more of `Set`, `Del`, `Add`, and
// `Rem`, bitwise OR-ed together.
//
// Notifications will not be sent for changes made as the result of applying a
// snapshot.
func (s *Store) Watch(path string, mask uint, events chan Event) {
	if path == "" && mask != Apply {
		return
	}
	ready := make(chan int)
	s.watchCh <- watch{events, mask, path, ready}
	<-ready
}

// Like `Watch`, but instead sends a placeholder event after every mutation
// application. The event's `Type` will be equal to `Apply`.
//
// This event will be sent after all normal Watch events for each seqn have
// been delivered. This gives you a reliable way to know when you have recieved
// all events for a given seqn (even if there were no such events).
//
// You probably don't want this. Use sparingly.
func (s *Store) WatchApply(evs chan Event) {
	s.Watch("", Apply, evs)
}
