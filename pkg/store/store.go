package store

import (
	"container/vector"
	"container/heap"
	"os"
	"strings"
)

type Event struct {
	Type int
	Path string
	Value string
}

const (
	Set = (1<<iota)
	Del
	Add
	Rem
)

var (
	BadPathError = os.NewError("bad path")
	BadMutationError = os.NewError("bad mutation")
)

type Store struct {
	applyCh chan apply
	reqCh chan req
}

type apply struct {
	seqn uint64
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

type applyHeap struct {
	vector.Vector
}

func (ah applyHeap) Less(i, j int) bool {
	return ah.Seqn(i) < ah.Seqn(j)
}

func (ah applyHeap) Seqn(i int) uint64 {
	return ah.At(i).(*apply).seqn
}

func NewStore() *Store {
	next := uint64(1)
	todo := new(applyHeap)
	heap.Init(todo)
	values := make(map[string]string)
	reqCh := make(chan req)
	applyCh := make(chan apply)
	go func() {
		for {
			select {
			case a := <-applyCh:
				heap.Push(todo, &a)
				for todo.Len() > 0 && todo.Seqn(0) == next {
					t := heap.Pop(todo).(*apply)
					values[t.k] = t.v
					next++
				}
			case r := <-reqCh:
				v, ok := values[r.k]
				r.ch <- reply{v, ok}
			}
		}
	}()
	return &Store{
		applyCh: applyCh,
		reqCh: reqCh,
	}
}

func Encode(path, v string) (mutation string, err os.Error) {
	switch {
	case len(path) < 1,
	     path[0] != '/',
	     strings.Count(path, "=") > 0,
	     strings.Count(path, " ") > 0:
		return "", BadPathError
	}
	return path + "=" + v, nil
}

func decode(mutation string) (path, v string, err os.Error) {
	parts := strings.Split(mutation, "=", 2)
	if len(parts) < 2 {
		return "", "", BadMutationError
	}
	return parts[0], parts[1], nil
}

func (s *Store) Apply(seqn uint64, mutation string) {
	path, v, err := decode(mutation)
	if err != nil {
		return
	}
	s.applyCh <- apply{seqn, path, v}
}

// For a missing path, `ok == false`. Otherwise, it is `true`.
func (s *Store) Lookup(path string) (v string, ok bool) {
	ch := make(chan reply)
	s.reqCh <- req{path, ch}
	rep := <-ch
	//v, ok = s.values[path]
	return rep.v, rep.ok
}

// `eventMask` is one or more of `Set`, `Del`, `Add`, and `Rem`, bitwise OR-ed
// together.
func (s *Store) Watch(path string, eventMask byte) (events chan Event) {
	return make(chan Event)
}
