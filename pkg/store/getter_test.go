package store

import (
	"doozer/assert"
	"testing"
)

func TestGetString(t *testing.T) {
	s := New()
	s.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	s.Sync(1)
	assert.Equal(t, "a", GetString(s, "/x"))
}

func TestGetStringMissing(t *testing.T) {
	s := New()
	assert.Equal(t, "", GetString(s, "/x"))
}

func TestGetStringDir(t *testing.T) {
	s := New()
	s.Ops <- Op{1, MustEncodeSet("/x/y", "a", Clobber)}
	s.Sync(1)
	assert.Equal(t, "", GetString(s, "/x"))
}

func TestGetDir(t *testing.T) {
	s := New()
	s.Ops <- Op{1, MustEncodeSet("/x/y", "a", Clobber)}
	s.Sync(1)
	assert.Equal(t, []string{"y"}, GetDir(s, "/x"))
}

func TestGetDirMissing(t *testing.T) {
	s := New()
	assert.Equal(t, []string(nil), GetDir(s, "/x"))
}

func TestGetDirString(t *testing.T) {
	s := New()
	s.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	s.Sync(1)
	assert.Equal(t, []string(nil), GetDir(s, "/x"))
}

func TestWalk(t *testing.T) {
	s := New()
	s.Ops <- Op{1, MustEncodeSet("/d/x", "1", Clobber)}
	s.Ops <- Op{2, MustEncodeSet("/d/y", "2", Clobber)}
	s.Ops <- Op{3, MustEncodeSet("/d/z/a", "3", Clobber)}
	s.Ops <- Op{4, MustEncodeSet("/m/y", "", Clobber)}
	s.Ops <- Op{5, MustEncodeSet("/n", "", Clobber)}
	s.Sync(5)
	ch := MustWalk(s, "/d/**")
	e := <-ch
	assert.Equal(t, "/d/x", e.Path)
	assert.Equal(t, "1", e.Body)
	e = <-ch
	assert.Equal(t, "/d/y", e.Path)
	assert.Equal(t, "2", e.Body)
	e = <-ch
	assert.Equal(t, "/d/z/a", e.Path)
	assert.Equal(t, "3", e.Body)
	<-ch
	assert.T(t, closed(ch))
}
