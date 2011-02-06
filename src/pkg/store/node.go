package store

import (
	"gob"
	"os"
	"strings"
)

var emptyDir = node{V: "", Ds: make(map[string]node), Cas: Dir}

const ErrorPath = "/store/error"

const Nop = "nop:"

// This structure should be kept immutable.
type node struct {
	V   string
	Cas int64
	Ds  map[string]node
}

func (n node) String() string {
	return "<node>"
}

func (n node) readdir() []string {
	names := make([]string, len(n.Ds))
	i := 0
	for name, _ := range n.Ds {
		names[i] = name
		i++
	}
	return names
}

func (n node) at(parts []string) (node, os.Error) {
	switch len(parts) {
	case 0:
		return n, nil
	default:
		if n.Ds != nil {
			if m, ok := n.Ds[parts[0]]; ok {
				return m.at(parts[1:])
			}
		}
		return node{}, os.ENOENT
	}
	panic("unreachable")
}

func (n node) get(parts []string) ([]string, int64) {
	switch m, err := n.at(parts); err {
	case os.ENOENT:
		return []string{""}, Missing
	default:
		if len(m.Ds) > 0 {
			return m.readdir(), m.Cas
		} else {
			return []string{m.V}, m.Cas
		}
	}
	panic("unreachable")
}

func (n node) Get(path string) ([]string, int64) {
	if err := checkPath(path); err != nil {
		return []string{""}, Missing
	}

	return n.get(split(path))
}

func (n node) stat(parts []string) (int32, int64) {
	switch m, err := n.at(parts); err {
	case os.ENOENT:
		return 0, Missing
	default:
		l := len(m.Ds)
		if l > 0 {
			return int32(l), m.Cas
		} else {
			return int32(len(m.V)), m.Cas
		}
	}
	panic("unreachable")
}

func (n node) Stat(path string) (int32, int64) {
	if err := checkPath(path); err != nil {
		return 0, Missing
	}

	return n.stat(split(path))
}


func copyMap(a map[string]node) map[string]node {
	b := make(map[string]node)
	for k, v := range a {
		b[k] = v
	}
	return b
}

// Return value is replacement node
func (n node) set(parts []string, v string, cas int64, keep bool) (node, bool) {
	if len(parts) == 0 {
		return node{v, cas, n.Ds}, keep
	}

	n.Ds = copyMap(n.Ds)
	p, ok := n.Ds[parts[0]].set(parts[1:], v, cas, keep)
	n.Ds[parts[0]] = p, ok
	n.Cas = Dir
	return n, len(n.Ds) > 0
}

func (n node) setp(k, v string, cas int64, keep bool) node {
	if err := checkPath(k); err != nil {
		return n
	}

	n, _ = n.set(split(k), v, cas, keep)
	return n
}

func (n node) apply(seqn int64, mut string) (rep node, ev Event, snap bool) {
	ev.Seqn, ev.Cas, ev.Mut = seqn, seqn, mut
	if seqn == 1 {
		d := gob.NewDecoder(strings.NewReader(mut))
		if d.Decode(&ev.Seqn) == nil {
			snap = true
			ev.Cas = dummy
			ev.Err = d.Decode(&rep)
			if ev.Err != nil {
				ev.Seqn = seqn
				rep = n
			}
			ev.Getter = rep
			return
		}
	}

	if mut == Nop {
		ev.Path = "/"
		ev.Cas = dummy
		rep = n
		ev.Getter = rep
		return
	}

	var cas int64
	var keep bool
	ev.Path, ev.Body, cas, keep, ev.Err = decode(mut)

	if ev.Err == nil && keep {
		components := split(ev.Path)
		for i := 0; i < len(components)-1; i++ {
			_, dirCas := n.get(components[0 : i+1])
			if dirCas == Missing {
				break
			}
			if dirCas != Dir {
				ev.Err = os.ENOTDIR
				break
			}
		}
	}

	if ev.Err == nil {
		_, curCas := n.Get(ev.Path)
		if cas != Clobber && cas != curCas {
			ev.Err = ErrCasMismatch
		} else if curCas == Dir {
			ev.Err = os.EISDIR
		}
	}

	if ev.Err != nil {
		ev.Path, ev.Body, cas, keep = ErrorPath, ev.Err.String(), Clobber, true
	}

	if !keep {
		ev.Cas = Missing
	}

	rep = n.setp(ev.Path, ev.Body, ev.Cas, keep)
	ev.Getter = rep
	return
}
