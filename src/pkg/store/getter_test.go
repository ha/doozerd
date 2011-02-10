package store

import (
	"github.com/bmizerany/assert"
	"sort"
	"testing"
)

func TestGetString(t *testing.T) {
	st := New()
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Sync(1)
	assert.Equal(t, "a", GetString(st, "/x"))
}

func TestGetStringMissing(t *testing.T) {
	st := New()
	assert.Equal(t, "", GetString(st, "/x"))
}

func TestGetStringDir(t *testing.T) {
	st := New()
	st.Ops <- Op{1, MustEncodeSet("/x/y", "a", Clobber)}
	st.Sync(1)
	assert.Equal(t, "", GetString(st, "/x"))
}

func TestGetdir(t *testing.T) {
	st := New()
	st.Ops <- Op{1, MustEncodeSet("/x/y", "a", Clobber)}
	st.Sync(1)
	assert.Equal(t, []string{"y"}, Getdir(st, "/x"))
}

func TestGetdirMissing(t *testing.T) {
	st := New()
	assert.Equal(t, []string(nil), Getdir(st, "/x"))
}

func TestGetdirString(t *testing.T) {
	st := New()
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Sync(1)
	assert.Equal(t, []string(nil), Getdir(st, "/x"))
}

func TestWalk(t *testing.T) {
	exp := map[string]string{
		"/d/x":   "1",
		"/d/y":   "2",
		"/d/z/a": "3",
	}
	var expPaths []string
	for p, _ := range exp {
		expPaths = append(expPaths, p)
	}
	sort.SortStrings(expPaths)

	st := New()
	st.Ops <- Op{1, MustEncodeSet("/d/x", "1", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/d/y", "2", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/d/z/a", "3", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/m/y", "", Clobber)}
	st.Ops <- Op{5, MustEncodeSet("/n", "", Clobber)}
	glob, err := CompileGlob("/d/**")
	assert.Equal(t, nil, err)
	var c int
	b := Walk(st, glob, func(path, body string, cas int64) bool {
		assert.Equal(t, expPaths[0], path)
		assert.Equal(t, exp[path], body)
		c++
		expPaths = expPaths[1:]
		return false
	})
	assert.Equal(t, false, b)
	assert.Equal(t, 3, c)
}

func TestWalkStop(t *testing.T) {
	exp := map[string]string{
		"/d/x":   "1",
		"/d/y":   "2",
		"/d/z/a": "3",
	}
	var expPaths []string
	for p, _ := range exp {
		expPaths = append(expPaths, p)
	}
	sort.SortStrings(expPaths)

	st := New()
	st.Ops <- Op{1, MustEncodeSet("/d/x", "1", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/d/y", "2", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/d/z/a", "3", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/m/y", "", Clobber)}
	st.Ops <- Op{5, MustEncodeSet("/n", "", Clobber)}
	glob, err := CompileGlob("/d/**")
	assert.Equal(t, nil, err)
	var c int
	b := Walk(st, glob, func(path, body string, cas int64) bool {
		assert.Equal(t, expPaths[0], path)
		assert.Equal(t, exp[path], body)
		c++
		expPaths = expPaths[1:]
		return true
	})
	assert.Equal(t, true, b)
	assert.Equal(t, 1, c)
}
