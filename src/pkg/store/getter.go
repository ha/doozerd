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

type Visitor func(path, body, cas string)

func walk(g Getter, path string, re *regexp.Regexp, f Visitor) {
	v, cas := g.Get(path)
	if cas != Dir && re.MatchString(path) {
		f(path, v[0], cas)
		return
	}

	if cas == Missing {
		return
	}

	if path == "/" {
		path = ""
	}

	for _, ent := range v {
		walk(g, path+"/"+ent, re, f)
	}
}

func Walk(g Getter, pattern string, f Visitor) os.Error {
	// TODO find the longest non-glob prefix of pattern and start there
	re, err := compileGlob(pattern)
	if err != nil {
		return err
	}

	walk(g, "/", re, f)
	return nil
}
