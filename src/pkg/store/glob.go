package store

import (
	"os"
	"regexp"
	"strings"
)

type Glob struct {
	s string
	r *regexp.Regexp
}

// Method String returns the original pattern used to create g.
func (g *Glob) String() string {
	return g.s
}

// Supports unix/ruby-style glob patterns:
//  - `?` matches a single char in a single path component
//  - `*` matches zero or more chars in a single path component
//  - `**` matches zero or more chars in zero or more components
func translateGlob(pat string) (regexp string) {
	outs := make([]string, len(pat))
	i, double := 0, false
	for _, c := range pat {
		switch c {
		default:
			outs[i] = string(c)
			double = false
		case '.', '+', '-', '^', '$', '[', ']', '(', ')':
			outs[i] = `\` + string(c)
			double = false
		case '?':
			outs[i] = `[^/]`
			double = false
		case '*':
			if double {
				outs[i-1] = `.*`
			} else {
				outs[i] = `[^/]*`
			}
			double = !double
		}
		i++
	}
	outs = outs[0:i]

	return "^" + strings.Join(outs, "") + "$"
}

// CompileGlob translates pat into a form more convenient for
// matching against paths in the store.
func CompileGlob(pat string) (*Glob, os.Error) {
	r, err := regexp.Compile(translateGlob(pat))
	if err != nil {
		return nil, err
	}
	return &Glob{pat, r}, nil
}

// MustCompileGlob is like CompileGlob, but it panics if an error occurs,
// simplifying safe initialization of global variables holding glob patterns.
func MustCompileGlob(pat string) *Glob {
	g, err := CompileGlob(pat)
	if err != nil {
		panic(err)
	}
	return g
}
