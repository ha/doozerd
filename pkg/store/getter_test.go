package store

import (
	"junta/assert"
	"testing"
)

func TestGetString(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Sync(1)
	assert.Equal(t, "a", GetString(s, "/x"))
}

func TestGetStringMissing(t *testing.T) {
	s := New()
	assert.Equal(t, "", GetString(s, "/x"))
}

func TestGetStringDir(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x/y", "a", Clobber))
	s.Sync(1)
	assert.Equal(t, "", GetString(s, "/x"))
}

func TestGetDir(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x/y", "a", Clobber))
	s.Sync(1)
	assert.Equal(t, []string{"y"}, GetDir(s, "/x"))
}

func TestGetDirMissing(t *testing.T) {
	s := New()
	assert.Equal(t, []string(nil), GetDir(s, "/x"))
}

func TestGetDirString(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Sync(1)
	assert.Equal(t, []string(nil), GetDir(s, "/x"))
}
