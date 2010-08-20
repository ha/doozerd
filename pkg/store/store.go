package store

import (
	"log"
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
	Nop = 0
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
	logger *log.Logger
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
	ready chan int
}

func New(logger *log.Logger) *Store {
	s := &Store{
		applyCh: make(chan apply),
		reqCh: make(chan req),
		watchCh: make(chan watch),
		todo: make(map[uint64]apply),
		watches: make(map[string][]watch),
		logger: logger,
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

func join(parts []string) string {
	return "/" + strings.Join(parts, "/")
}

func (n node) getp(path string) (string, bool) {
	if err := checkPath(path); err != nil {
		return "", false
	}

	return n.get(split(path))
}

// Return value: y = replacement node; c = how many levels were changed.
func (n node) set(parts []string, v string, keep bool) (y *node, c int) {
	switch len(parts) {
	case 0:
		n.v = v
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

func EncodeSet(path, body string) (mutation string, err os.Error) {
	if err = checkPath(path); err != nil {
		return
	}
	return path + "=" + body, nil
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

func (s *Store) notify(t uint, seqn uint64, k, v string) {
	for _, w := range s.watches[k] {
		if w.mask & t != 0 {
			go func(ch chan Event, ev Event) {
				ch <- ev
			}(w.ch, Event{t, seqn, k, v})
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
				if t.op != Nop {
					var changed []string
					s.notify(t.op, t.seqn, t.k, t.v)
					values, changed = values.setp(t.k, t.v, t.op == Set)
					s.logger.Logf("store applied %v", t)
					for _, p := range changed {
						dirname, basename := path.Split(p)
						if dirname != "/" {
							dirname = dirname[0:len(dirname) - 1] // strip slash
						}
						s.notify(conj[t.op], t.seqn, dirname, basename)
					}
				}
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
			w.ready <- 1
		}
	}
}

func (s *Store) Apply(seqn uint64, mutation string) {
	op, path, v, err := decode(mutation)
	if err != nil {
		s.applyCh <- apply{seqn:seqn} // nop
	} else {
		s.applyCh <- apply{seqn, op, path, v}
	}
}

// For a missing path, `ok == false`. Otherwise, it is `true`.
func (s *Store) Lookup(path string) (body string, ok bool) {
	ch := make(chan reply)
	s.reqCh <- req{path, ch}
	rep := <-ch
	return rep.v, rep.ok
}

// `mask` is one or more of `Set`, `Del`, `Add`, and `Rem`, bitwise OR-ed
// together.
func (s *Store) Watch(path string, mask uint) (events chan Event) {
	ch := make(chan Event)
	ready := make(chan int)
	s.watchCh <- watch{ch, mask, path, ready}
	<-ready
	return ch
}
