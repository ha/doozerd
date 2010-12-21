package store

import (
	"github.com/bmizerany/assert"
	"testing"
)

var globs = [][]string{
	{"/", `^/$`},
	{"/a", `^/a$`},
	{"/a.b", `^/a\.b$`},
	{"/a-b", `^/a\-b$`},
	{"/a?", `^/a[^/]$`},
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

var dontCompile = []string{
	"",
	"a",
	"a/",
	"/ ",
	"/:",
	"//",
	"/a/",
	"/a+b",
	"/a^b",
	"/a$b",
	"/a[b",
	"/a]b",
	"/a(b",
	"/a)b",
	"/a世界",
}

func TestGlobTranslateOk(t *testing.T) {
	for _, parts := range globs {
		pat, exp := parts[0], parts[1]
		got, err := translateGlob(pat)
		if got != exp {
			t.Errorf("expected %q, but got %q from %q", exp, got, pat)
		}
		if err != nil {
			t.Errorf("in %q, unexpected err %v", pat, err)
		}
	}
}

func TestGlobTranslateError(t *testing.T) {
	for _, pat := range dontCompile {
		re, err := translateGlob(pat)
		if err == nil {
			t.Errorf("pat %q shouldn't translate, but got %q", pat, re)
			continue
		}

		glob, err := CompileGlob(pat)
		if err == nil {
			t.Errorf("pat %q shouldn't compile, but got %#v", pat, glob)
		}
	}
}

func TestGlobMatches(t *testing.T) {
	for _, parts := range matches {
		pat, paths := parts[0], parts[1:]
		glob, err := CompileGlob(pat)
		assert.Equal(t, nil, err)
		for _, path := range paths {
			if !glob.Match(path) {
				t.Errorf("pat %q should match %q", pat, path)
			}
		}
	}
}

func TestGlobNonMatches(t *testing.T) {
	for _, parts := range nonMatches {
		pat, paths := parts[0], parts[1:]
		glob, err := CompileGlob(pat)
		assert.Equal(t, nil, err)
		for _, path := range paths {
			if glob.Match(path) {
				t.Errorf("pat %q should not match %q", pat, path)
			}
		}
	}
}
