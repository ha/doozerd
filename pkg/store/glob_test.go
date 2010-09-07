package store

import (
	"junta/assert"
	"testing"
)

var globs = [][]string{
	[]string{"/", `^/$`},
	[]string{"/a", `^/a$`},
	[]string{"/a.b", `^/a\.b$`},
	[]string{"/a世界", `^/a世界$`},
	[]string{"/a?", `^/a[^/]$`},
	[]string{"/a/", `^/a/$`},
	[]string{"/a/b", `^/a/b$`},
	[]string{"/*", `^/[^/]*$`},
	[]string{"/*/a", `^/[^/]*/a$`},
	[]string{"/*a/b", `^/[^/]*a/b$`},
	[]string{"/a*/b", `^/a[^/]*/b$`},
	[]string{"/a*a/b", `^/a[^/]*a/b$`},
	[]string{"/*a*/b", `^/[^/]*a[^/]*/b$`},
	[]string{"/**", `^/.*$`},
	[]string{"/**/a", `^/.*/a$`},
}

var matches = [][]string{
	[]string{"/a/b", "/a/b"},
	[]string{"/a?", "/ab", "/ac"},
	[]string{"/a*", "/a", "/ab", "/abc"},
	[]string{"/a**", "/a", "/ab", "/abc", "/a/", "/a/b", "/ab/c"},
}

var nonMatches = [][]string{
	[]string{"/a/b", "/a/c", "/a/", "/a/b/", "/a/bc"},
	[]string{"/a?", "/", "/abc", "/a", "/a/"},
	[]string{"/a*", "/", "/a/", "/ba"},
	[]string{"/a**", "/", "/ba"},
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
