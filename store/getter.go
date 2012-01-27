package store

import (
	"sort"
)

type Getter interface {
	Get(path string) (values []string, rev int64)
	Stat(path string) (ln int32, rev int64)
}

// Retrieves the body stored in `g` at `path` and returns it. If `path` is a
// directory or does not exist, returns an empty string.
//
// Note, with this function it is impossible to distinguish between an empty
// string stored at `path`, a missing entry, and a directory. If you need to
// tell the difference, use `g.Get`.
//
// Also note, this function does not return the revision for `path`. If you
// need the revision, use `g.Get`.
func GetString(g Getter, path string) (body string) {
	v, rev := g.Get(path)
	if rev == Missing || rev == Dir {
		return ""
	}
	return v[0]
}

// Returns a list of entries in `g` in the directory at `path`. If `path` is
// not a directory, returns an empty slice.
//
// Note, with this function it is impossible to distinguish between a string
// stored at `path` and a missing entry. If you need to tell the difference,
// use `g.Get`.
func Getdir(g Getter, path string) (entries []string) {
	v, rev := g.Get(path)
	if rev != Dir {
		return nil
	}
	return v
}

type Visitor func(path, body string, rev int64) (stop bool)

func walk(g Getter, path string, glob *Glob, f Visitor) (stopped bool) {
	v, rev := g.Get(path)
	if rev == Missing {
		return
	}

	if rev != Dir {
		return glob.Match(path) && f(path, v[0], rev)
	}

	if path == "/" {
		path = ""
	}

	sort.Strings(v)
	for _, ent := range v {
		stopped = walk(g, path+"/"+ent, glob, f)
		if stopped {
			return
		}
	}
	return
}

// Walk walks the entries in g, calling f for each file that matches glob.
// Entries are visited in sorted order.
// If f returns true, Walk will stop visiting entries and return immediately;
// Walk won't call f again.
// Walk returns true if f returned true.
func Walk(g Getter, glob *Glob, f Visitor) (stopped bool) {
	// TODO find the longest non-glob prefix of glob.Pattern and start there
	return walk(g, "/", glob, f)
}
