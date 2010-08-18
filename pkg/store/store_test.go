package store

import (
	"borg/assert"
	"testing"
)

var KVMs = [][3]string{
	[3]string{"/", "a", "/=a"},
	[3]string{"/x", "a", "/x=a"},
	[3]string{"/x", "a=b", "/x=a=b"},
	[3]string{"/x", "a b", "/x=a b"},
}

var BadPaths = []string{
	"",
	"x",
	"/x=",
	"/x y",
}

var BadMutations = []string{
	"x",
}

func TestEncode(t *testing.T) {
	for _, kvm := range KVMs {
		k, v, exp := kvm[0], kvm[1], kvm[2]
		got, err := Encode(k, v)
		if err != nil {
			t.Error("unexpected error:", err)
		}
		assert.Equal(t, exp, got, "")
	}
}

func TestEncodeBadPaths(t *testing.T) {
	for _, k := range BadPaths {
		_, err := Encode(k, "")
		if err != BadPathError {
			t.Errorf("expected BadPathError on %q", k)
		}
	}
}

func TestDecode(t *testing.T) {
	for _, kvm := range KVMs {
		expk, expv, m := kvm[0], kvm[1], kvm[2]
		gotk, gotv, err := decode(m)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, expv, gotv, "value from " + m)
	}
}

func TestDecodeBadMutations(t *testing.T) {
	for _, m := range BadMutations {
		_, _, err := decode(m)
		if err != BadMutationError {
			t.Errorf("expected BadMutationError on %q", m)
		}
	}
}

func TestApply(t *testing.T) {
	s := NewStore()
	mut, _ := Encode("/x", "a")
	s.Apply(1, mut)
}

func TestLookupMissing(t *testing.T) {
	s := NewStore()
	v, ok := s.Lookup("/x")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "")
}

func TestLookup(t *testing.T) {
	s := NewStore()
	mut, _ := Encode("/x", "a")
	s.Apply(1, mut)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "a", v, "")
}

func TestApplyInOrder(t *testing.T) {
	s := NewStore()
	mut1, _ := Encode("/x", "a")
	mut2, _ := Encode("/x", "b")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestApplyOutOfOrder(t *testing.T) {
	s := NewStore()
	mut1, _ := Encode("/x", "a")
	mut2, _ := Encode("/x", "b")
	s.Apply(2, mut2)
	s.Apply(1, mut1)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestApplyIgnoreDuplicate(t *testing.T) {
	s := NewStore()
	mut1, _ := Encode("/x", "a")
	mut2, _ := Encode("/x", "b")
	s.Apply(1, mut1)
	s.Apply(1, mut2)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "a", v, "")

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo), "")
}

func TestApplyIgnoreDuplicateOutOfOrder(t *testing.T) {
	s := NewStore()
	mut1, _ := Encode("/x", "a")
	mut2, _ := Encode("/x", "b")
	mut3, _ := Encode("/x", "c")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(1, mut3)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo), "")
}

func TestWatchSet(t *testing.T) {
	s := NewStore()

	ch := s.Watch("/x", Set)
	assert.Equal(t, 1, len(s.watches["/x"]), "")

	mut1, _ := Encode("/x", "a")
	mut2, _ := Encode("/x", "b")
	mut3, _ := Encode("/y", "c")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)

	expa := <-ch
	assert.Equal(t, Event{Set, 1, "/x", "a"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Set, 2, "/x", "b"}, expb, "")
}

func TestWatchAdd(t *testing.T) {
	s := NewStore()

	ch := s.Watch("/", Add)
	assert.Equal(t, 1, len(s.watches["/"]), "")

	mut1, _ := Encode("/x", "a")
	mut2, _ := Encode("/x", "b")
	mut3, _ := Encode("/y", "c")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)

	expa := <-ch
	assert.Equal(t, Event{Add, 1, "/", "x"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Add, 3, "/", "y"}, expb, "")
}
