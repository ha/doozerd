package store

import (
	"testing"
)

var globs = [][]string{
	[]string{"/", `^/$`},
	[]string{"/a", `^/a$`},
	[]string{"/a.b", `^/a\.b$`},
	[]string{"/a世界", `^/a世界$`},
	[]string{"/a?", `^/a.$`},
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

func TestTranslateGlob(t *testing.T) {
	for _, parts := range globs {
		pat, exp := parts[0], parts[1]
		got := translateGlob(pat)
		if got != exp {
			t.Errorf("expected %q, but got %q from %q", exp, got, pat)
		}
	}
}
