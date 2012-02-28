package store

import (
	"syscall"
)

var emptyDir = node{V: "", Ds: make(map[string]node), Rev: Dir}

const ErrorPath = "/ctl/err"

const Nop = "nop:"

// This structure should be kept immutable.
type node struct {
	V   string
	Rev int64
	Ds  map[string]node
}

func (n node) String() string {
	return "<node>"
}

func (n node) readdir() []string {
	names := make([]string, len(n.Ds))
	i := 0
	for name := range n.Ds {
		names[i] = name
		i++
	}
	return names
}

func (n node) at(parts []string) (node, error) {
	switch len(parts) {
	case 0:
		return n, nil
	default:
		if n.Ds != nil {
			if m, ok := n.Ds[parts[0]]; ok {
				return m.at(parts[1:])
			}
		}
		return node{}, syscall.ENOENT
	}
	panic("unreachable")
}

func (n node) get(parts []string) ([]string, int64) {
	switch m, err := n.at(parts); err {
	case syscall.ENOENT:
		return []string{""}, Missing
	default:
		if len(m.Ds) > 0 {
			return m.readdir(), m.Rev
		} else {
			return []string{m.V}, m.Rev
		}
	}
	panic("unreachable")
}

func (n node) Get(path string) ([]string, int64) {
	return n.get(split(path))
}

func (n node) stat(parts []string) (int32, int64) {
	switch m, err := n.at(parts); err {
	case syscall.ENOENT:
		return 0, Missing
	default:
		l := len(m.Ds)
		if l > 0 {
			return int32(l), m.Rev
		} else {
			return int32(len(m.V)), m.Rev
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
func (n node) set(parts []string, v string, rev int64, keep bool) (node, bool) {
	if len(parts) == 0 {
		return node{v, rev, n.Ds}, keep
	}

	n.Ds = copyMap(n.Ds)
	p, ok := n.Ds[parts[0]].set(parts[1:], v, rev, keep)
	if ok {
		n.Ds[parts[0]] = p
	} else {
		delete(n.Ds, parts[0])
	}
	n.Rev = Dir
	return n, len(n.Ds) > 0
}

func (n node) setp(k, v string, rev int64, keep bool) node {
	if err := checkPath(k); err != nil {
		return n
	}

	n, _ = n.set(split(k), v, rev, keep)
	return n
}

func (n node) apply(seqn int64, mut string) (rep node, ev Event) {
	ev.Seqn, ev.Rev, ev.Mut = seqn, seqn, mut
	if mut == Nop {
		ev.Path = "/"
		ev.Rev = nop
		rep = n
		ev.Getter = rep
		return
	}

	var rev int64
	var keep bool
	ev.Path, ev.Body, rev, keep, ev.Err = decode(mut)

	if ev.Err == nil && keep {
		components := split(ev.Path)
		for i := 0; i < len(components)-1; i++ {
			_, dirRev := n.get(components[0 : i+1])
			if dirRev == Missing {
				break
			}
			if dirRev != Dir {
				ev.Err = syscall.ENOTDIR
				break
			}
		}
	}

	if ev.Err == nil {
		_, curRev := n.Get(ev.Path)
		if rev != Clobber && rev < curRev {
			ev.Err = ErrRevMismatch
		} else if curRev == Dir {
			ev.Err = syscall.EISDIR
		}
	}

	if ev.Err != nil {
		ev.Path, ev.Body, rev, keep = ErrorPath, ev.Err.Error(), Clobber, true
	}

	if !keep {
		ev.Rev = Missing
	}

	rep = n.setp(ev.Path, ev.Body, ev.Rev, keep)
	ev.Getter = rep
	return
}
