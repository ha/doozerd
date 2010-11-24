package store

import (
	"assert"
	"testing"
)

var globs = [][]string{
	{"/", `^/$`},
	{"/a", `^/a$`},
	{"/a.b", `^/a\.b$`},
	{"/a世界", `^/a世界$`},
	{"/a?", `^/a[^/]$`},
	{"/a/", `^/a/$`},
	{"/a/b", `^/a/b$`},
	{"/*", `^/[^/]*$`},
	{"/*/a", `^/[^/]*/a$`},
	{"/*a/b", `^/[^/]*a/b$`},
	{"/a*/b", `^/a[^/]*/b$`},
	{"/a*a/b", `^/a[^/]*a/b$`},
	{"/*a*/b", `^/[^/]*a[^/]*/b$`},
	{"/**", `^/.*$`},
	{"/**/a", `^/.*/a$`},
}

var matches = [][]string{
	{"/a/b", "/a/b"},
	{"/a?", "/ab", "/ac"},
	{"/a*", "/a", "/ab", "/abc"},
	{"/a**", "/a", "/ab", "/abc", "/a/", "/a/b", "/ab/c"},
}

var nonMatches = [][]string{
	{"/a/b", "/a/c", "/a/", "/a/b/", "/a/bc"},
	{"/a?", "/", "/abc", "/a", "/a/"},
	{"/a*", "/", "/a/", "/ba"},
	{"/a**", "/", "/ba"},
}

func TestGlobTranslate(t *testing.T) {
	for _, parts := range globs {
		pat, exp := parts[0], parts[1]
		got := translateGlob(pat)
		if got != exp {
			t.Errorf("expected %q, but got %q from %q", exp, got, pat)
		}
	}
}

func TestGlobMatches(t *testing.T) {
	for _, parts := range matches {
		pat, paths := parts[0], parts[1:]
		re, err := compileGlob(pat)
		assert.Equal(t, nil, err)
		for _, path := range paths {
			if !re.MatchString(path) {
				t.Errorf("pat %q should match %q", pat, path)
			}
		}
	}
}

func TestGlobNonMatches(t *testing.T) {
	for _, parts := range nonMatches {
		pat, paths := parts[0], parts[1:]
		re, err := compileGlob(pat)
		assert.Equal(t, nil, err)
		for _, path := range paths {
			if re.MatchString(path) {
				t.Errorf("pat %q should not match %q", pat, path)
			}
		}
	}
}
