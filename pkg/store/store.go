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

var (
	BadPathError = os.NewError("bad path")
)

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

func checkPath(k string) os.Error {
	switch {
	case len(k) < 1,
	     k[0] != '/',
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
	values := make(map[string]string)
	for {
		select {
		case a := <-s.applyCh:
			if a.seqn >= next {
				s.todo[a.seqn] = a
			}
			for t, ok := s.todo[next]; ok; t, ok = s.todo[next] {
				go s.notify(t.op, a.seqn, t.k, t.v)
				switch t.op {
				case Set:
					if _, ok := values[t.k]; !ok {
						dirname, basename := path.Split(t.k)
						go s.notify(Add, a.seqn, dirname, basename)
					}
					values[t.k] = t.v
				case Del:
					if _, ok := values[t.k]; ok {
						dirname, basename := path.Split(t.k)
						go s.notify(Rem, a.seqn, dirname, basename)
					}
					values[t.k] = "", false
				}
				s.todo[next] = apply{}, false
				next++
			}
		case r := <-s.reqCh:
			v, ok := values[r.k]
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
