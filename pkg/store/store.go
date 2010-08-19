package store

import (
	"os"
	"path"
	"strings"
)

type Event struct {
	Type uint
	Seqn uint64
	Path string
	Value string
}

const (
	Set = uint(1<<iota)
	Del
	Add
	Rem
)

var conj = map[uint]uint{Set:Add, Del:Rem}

var (
	BadPathError = os.NewError("bad path")
)

// This structure should be kept immutable.
type node struct {
	v string
	ds map[string]*node
}

var emptyNode = node{v:"", ds:make(map[string]*node)}

type Store struct {
	applyCh chan apply
	reqCh chan req
	watchCh chan watch
	watches map[string][]watch
	todo map[uint64]apply
}

type apply struct {
	seqn uint64
	op uint
	k string
	v string
}

type req struct {
	k string
	ch chan reply
}

type reply struct {
	v string
	ok bool
}

type watch struct {
	ch chan Event
	mask uint
	k string
}

func NewStore() *Store {
	s := &Store{
		applyCh: make(chan apply),
		reqCh: make(chan req),
		watchCh: make(chan watch),
		todo: make(map[uint64]apply),
		watches: make(map[string][]watch),
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

func (n node) getp(path string) (string, bool) {
	if err := checkPath(path); err != nil {
		return "", false
	}

	return n.get(split(path))
}

// Return value: replacement node.
func (n node) set(parts []string, v string, keep bool) (y *node) {
	switch len(parts) {
	case 0:
		n.v = v
		y = &node{v:v, ds:n.ds}
	default:
		m := n.ds[parts[0]]
		if m == nil {
			m = &emptyNode
		}
		m = m.set(parts[1:], v, keep)
		ds := make(map[string]*node)
		for k,v := range n.ds {
			ds[k] = v
		}
		ds[parts[0]] = m, m != nil
		y = &node{v:n.v, ds:ds}
	}
	if !keep && len(y.ds) == 0 {
		return nil
	}
	return
}

func (n node) setp(path string, v string, keep bool) (y node) {
	if err := checkPath(path); err != nil {
		return n
	}

	r := n.set(split(path), v, keep)
	if r == nil {
		return emptyNode
	}
	return *r
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

func EncodeSet(path, v string) (mutation string, err os.Error) {
	if err = checkPath(path); err != nil {
		return
	}
	return path + "=" + v, nil
}

func EncodeDel(path string) (mutation string, err os.Error) {
	if err := checkPath(path); err != nil {
		return
	}
	return path, nil
}

func decode(mutation string) (op uint, path, v string, err os.Error) {
	parts := strings.Split(mutation, "=", 2)
	if err = checkPath(parts[0]); err != nil {
		return
	}
	switch len(parts) {
	case 1:
		return Del, parts[0], "", nil
	case 2:
		return Set, parts[0], parts[1], nil
	}
	panic("can't happen")
}

func (s *Store) notify(ev uint, seqn uint64, k, v string) {
	for _, w := range s.watches[k] {
		if w.mask & ev != 0 {
			w.ch <- Event{ev, seqn, k, v}
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

func (s *Store) process() {
	next := uint64(1)
	values := emptyNode
	for {
		select {
		case a := <-s.applyCh:
			if a.seqn >= next {
				s.todo[a.seqn] = a
			}
			for t, ok := s.todo[next]; ok; t, ok = s.todo[next] {
				go s.notify(t.op, a.seqn, t.k, t.v)
				if _, ok := values.getp(t.k); ok == (t.op == Del) {
					dirname, basename := path.Split(t.k)
					go s.notify(conj[t.op], a.seqn, dirname, basename)
				}
				values = values.setp(t.k, t.v, t.op == Set)
				s.todo[next] = apply{}, false
				next++
			}
		case r := <-s.reqCh:
			v, ok := values.getp(r.k)
			r.ch <- reply{v, ok}
		case w := <-s.watchCh:
			watches := s.watches[w.k]
			append(&watches, w)
			s.watches[w.k] = watches
		}
	}
}

func (s *Store) Apply(seqn uint64, mutation string) {
	op, path, v, err := decode(mutation)
	if err != nil {
		return
	}
	s.applyCh <- apply{seqn, op, path, v}
}

// For a missing path, `ok == false`. Otherwise, it is `true`.
func (s *Store) Lookup(path string) (v string, ok bool) {
	ch := make(chan reply)
	s.reqCh <- req{path, ch}
	rep := <-ch
	return rep.v, rep.ok
}

// `mask` is one or more of `Set`, `Del`, `Add`, and `Rem`, bitwise OR-ed
// together.
func (s *Store) Watch(path string, mask uint) (events chan Event) {
	ch := make(chan Event)
	s.watchCh <- watch{ch, mask, path}
	return ch
}
