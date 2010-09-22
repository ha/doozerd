package store

import (
	"gob"
	"os"
	"strconv"
	"strings"
)

var root = node{v:"", ds:make(map[string]node), cas:Dir}

const ErrorPath = "/store/error"

const Nop = "nop:"

// This structure should be kept immutable.
type node struct {
	v string
	cas string
	ds map[string]node
}

func (n node) readdir() []string {
	names := make([]string, len(n.ds))
	i := 0
	for name, _ := range n.ds {
		names[i] = name
		i++
	}
	return names
}

func (n node) get(parts []string) ([]string, string) {
	switch len(parts) {
	case 0:
		if len(n.ds) > 0 {
			return n.readdir(), n.cas
		} else {
			return []string{n.v}, n.cas
		}
	default:
		if n.ds != nil {
			if m, ok := n.ds[parts[0]]; ok {
				return m.get(parts[1:])
			}
		}
		return []string{""}, Missing
	}
	panic("can't happen")
}

func (n node) getp(path string) ([]string, string) {
	if err := checkPath(path); err != nil {
		return []string{""}, Missing
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

func (n node) apply(seqn uint64, mut string) (rep node, ev Event) {
	ev.Seqn, ev.Cas, ev.Mut = seqn, strconv.Uitoa64(seqn), mut
	if seqn == 1 {
		d := gob.NewDecoder(strings.NewReader(mut))
		if d.Decode(&ev.Seqn) == nil {
			ev.Cas = ""
			ev.Err = d.Decode(&rep)
			if ev.Err != nil {
				ev.Seqn = seqn + 1
				rep = n
			}
			return
		}
	}

	if mut == Nop {
		ev.Cas = ""
		rep = n
		return
	}

	cas, keep := "", false
	ev.Path, ev.Body, cas, keep, ev.Err = decode(mut)

	if ev.Err == nil && keep {
		components := split(ev.Path)
		for i := 0; i < len(components) - 1; i++ {
			_, dirCas := n.get(components[0:i+1])
			if dirCas != Missing && dirCas != Dir {
				ev.Err = os.ENOTDIR
				break
			}
		}
	}

	if ev.Err == nil && cas != Clobber {
		_, curCas := n.getp(ev.Path)
		if cas != curCas {
			ev.Err = ErrCasMismatch
		}
	}

	if ev.Err != nil {
		ev.Path, ev.Body, cas, keep = ErrorPath, ev.Err.String(), Clobber, true
	}

	if !keep {
		ev.Cas = Missing
	}

	rep = n.setp(ev.Path, ev.Body, ev.Cas, keep)
	return
}
