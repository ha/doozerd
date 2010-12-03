package store

import (
	"os"
	"regexp"
)

type Getter interface {
	Get(path string) (values []string, cas string)
}

// Retrieves the body stored in `g` at `path` and returns it. If `path` is a
// directory or does not exist, returns an empty string.
//
// Note, with this function it is impossible to distinguish between an empty
// string stored at `path`, a missing entry, and a directory. If you need to
// tell the difference, use `g.Get`.
//
// Also note, this function does not return the CAS token for `path`. If you
// need the CAS token, use `g.Get`.
func GetString(g Getter, path string) (body string) {
	v, cas := g.Get(path)
	if cas == Missing || cas == Dir {
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
func GetDir(g Getter, path string) (entries []string) {
	v, cas := g.Get(path)
	if cas != Dir {
		return nil
	}
	return v
}

func walk(g Getter, path string, re *regexp.Regexp, ch chan Event) {
	v, cas := g.Get(path)
	if cas != Dir && re.MatchString(path) {
		ch <- Event{0, path, v[0], cas, "", nil, nil}
		return
	}

	if cas == Missing {
		return
	}

	if path == "/" {
		path = ""
	}

	for _, ent := range v {
		walk(g, path+"/"+ent, re, ch)
	}
}

func Walk(g Getter, pattern string) (chan Event, os.Error) {
	ch := make(chan Event)

	// TODO find the longest non-glob prefix of pattern and start there
	re, err := compileGlob(pattern)
	if err != nil {
		close(ch)
		return nil, err
	}

	go func() {
		walk(g, "/", re, ch)
		close(ch)
	}()

	return ch, nil
}

func MustWalk(g Getter, pattern string) chan Event {
	ch, err := Walk(g, pattern)
	if err != nil {
		panic(err)
	}
	return ch
}
