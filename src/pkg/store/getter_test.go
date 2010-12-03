package store

import (
	"github.com/bmizerany/assert"
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

func TestGetDir(t *testing.T) {
	st := New()
	st.Ops <- Op{1, MustEncodeSet("/x/y", "a", Clobber)}
	st.Sync(1)
	assert.Equal(t, []string{"y"}, GetDir(st, "/x"))
}

func TestGetDirMissing(t *testing.T) {
	st := New()
	assert.Equal(t, []string(nil), GetDir(st, "/x"))
}

func TestGetDirString(t *testing.T) {
	st := New()
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Sync(1)
	assert.Equal(t, []string(nil), GetDir(st, "/x"))
}

func TestWalk(t *testing.T) {
	exp := map[string]string{
		"/d/x":  "1",
		"/d/y":  "2",
		"/d/z/a":"3",
	}

	st := New()
	st.Ops <- Op{1, MustEncodeSet("/d/x", "1", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/d/y", "2", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/d/z/a", "3", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/m/y", "", Clobber)}
	st.Ops <- Op{5, MustEncodeSet("/n", "", Clobber)}
	for e := range MustWalk(st, "/d/**") {
		expBody, ok := exp[e.Path]
		assert.T(t, ok)
		exp[e.Path] = "", false
		assert.Equal(t, expBody, e.Body)
	}
	assert.Equal(t, 0, len(exp))
}
