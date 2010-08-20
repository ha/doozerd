package store

import (
	"borg/assert"
	"log"
	"testing"
	"testing/iotest"
)

var SetKVMs = [][3]string{
	[3]string{"/", "a", "/=a"},
	[3]string{"/x", "a", "/x=a"},
	[3]string{"/x", "a=b", "/x=a=b"},
	[3]string{"/x", "a b", "/x=a b"},
}

var DelKVMs = [][3]string{
	[3]string{"/", "/"},
	[3]string{"/x", "/x"},
}

var GoodPaths = []string{
	"/",
	"/x",
	"/x/y",
}

var BadPaths = []string{
	"",
	"x",
	"/x=",
	"/x y",
	"/x/",
}

var BadMutations = []string{
	"",
	"x",
	"/x y",
	"=",
	"x=",
	"/x y=",
}

var Splits = [][]string{
	[]string{"/"},
	[]string{"/x", "x"},
	[]string{"/x/y/z", "x", "y", "z"},
}

var logger = log.New(iotest.TruncateWriter(nil, 0), nil, "", log.Lok)

func TestSplit(t *testing.T) {
	for _, vals := range Splits {
		path, exp := vals[0], vals[1:]
		got := split(path)
		assert.Equal(t, exp, got, path)
	}
}

func TestCheckBadPaths(t *testing.T) {
	for _, k := range BadPaths {
		err := checkPath(k)
		if err != BadPathError {
			t.Errorf("expected BadPathError on %q", k)
		}
	}
}

func TestCheckGoodPaths(t *testing.T) {
	for _, k := range GoodPaths {
		err := checkPath(k)
		if err != nil {
			t.Errorf("unexpected error on %q: %v", k, err)
		}
	}
}

func TestEncodeSet(t *testing.T) {
	for _, kvm := range SetKVMs {
		k, v, exp := kvm[0], kvm[1], kvm[2]
		got, err := EncodeSet(k, v)
		if err != nil {
			t.Error("unexpected error:", err)
		}
		assert.Equal(t, exp, got, "")
	}
}

func TestEncodeDel(t *testing.T) {
	for _, kvm := range DelKVMs {
		k, exp := kvm[0], kvm[1]
		got, err := EncodeDel(k)
		if err != nil {
			t.Error("unexpected error:", err)
		}
		assert.Equal(t, exp, got, "")
	}
}

func TestDecodeSet(t *testing.T) {
	for _, kvm := range SetKVMs {
		expk, expv, m := kvm[0], kvm[1], kvm[2]
		op, gotk, gotv, err := decode(m)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, Set, op, "op from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, expv, gotv, "value from " + m)
	}
}

func TestDecodeDel(t *testing.T) {
	for _, kvm := range DelKVMs {
		expk, m := kvm[0], kvm[1]
		op, gotk, gotv, err := decode(m)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, Del, op, "op from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, "", gotv, "value from " + m)
	}
}

func TestDecodeBadMutations(t *testing.T) {
	for _, m := range BadMutations {
		_, _, _, err := decode(m)
		if err != BadPathError {
			t.Errorf("expected BadPathError on %q", m)
		}
	}
}

func TestLookupMissing(t *testing.T) {
	s := New(logger)
	v, ok := s.Lookup("/x")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "")
}

func TestLookup(t *testing.T) {
	s := New(logger)
	mut, _ := EncodeSet("/x", "a")
	s.Apply(1, mut)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "a", v, "")
}

func TestLookupDeleted(t *testing.T) {
	s := New(logger)
	mut, _ := EncodeSet("/x", "a")
	s.Apply(1, mut)
	mut, _ = EncodeDel("/x")
	s.Apply(2, mut)
	v, ok := s.Lookup("/x")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "")
}

func TestApplyInOrder(t *testing.T) {
	s := New(logger)
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestApplyBadThenGood(t *testing.T) {
	s := New(logger)
	mut1 := "foo" // bad mutation
	mut2, _ := EncodeSet("/x", "b")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestApplyOutOfOrder(t *testing.T) {
	s := New(logger)
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s.Apply(2, mut2)
	s.Apply(1, mut1)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestApplyIgnoreDuplicate(t *testing.T) {
	s := New(logger)
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s.Apply(1, mut1)
	s.Apply(1, mut2)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "a", v, "")

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo), "")
}

func TestApplyIgnoreDuplicateOutOfOrder(t *testing.T) {
	s := New(logger)
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/x", "c")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(1, mut3)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo), "")
}

func TestGetDir(t *testing.T) {
	s := New(logger)

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/y", "b")
	s.Apply(1, mut1)
	s.Apply(2, mut2)

	v, ok := s.Lookup("/")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "x\ny\n", v, "")
}

func TestDirParents(t *testing.T) {
	s := New(logger)

	mut1, _ := EncodeSet("/x/y/z", "a")
	s.Apply(1, mut1)

	v, ok := s.Lookup("/")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "x\n", v, "")

	v, ok = s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "y\n", v, "")

	v, ok = s.Lookup("/x/y")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "z\n", v, "")

	v, ok = s.Lookup("/x/y/z")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "a", v, "")
}

func TestDelDirParents(t *testing.T) {
	s := New(logger)

	mut1, _ := EncodeSet("/x/y/z", "a")
	s.Apply(1, mut1)

	mut2, _ := EncodeDel("/x/y/z")
	s.Apply(2, mut2)

	v, ok := s.Lookup("/")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "", v, "lookup /")

	v, ok = s.Lookup("/x")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "lookup /x")

	v, ok = s.Lookup("/x/y")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "lookup /x/y")

	v, ok = s.Lookup("/x/y/z")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "lookup /x/y/z")
}

func TestWatchSet(t *testing.T) {
	s := New(logger)

	ch := s.Watch("/x", Set)
	assert.Equal(t, 1, len(s.watches["/x"]), "")

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/y", "c")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)

	expa := <-ch
	assert.Equal(t, Event{Set, 1, "/x", "a"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Set, 2, "/x", "b"}, expb, "")
}

func TestWatchDel(t *testing.T) {
	s := New(logger)

	ch := s.Watch("/x", Del)
	assert.Equal(t, 1, len(s.watches["/x"]), "")

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/y", "c")
	mut4, _ := EncodeDel("/x")
	mut5, _ := EncodeDel("/y")
	mut6, _ := EncodeDel("/x")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)

	exp := <-ch
	assert.Equal(t, Event{Del, 4, "/x", ""}, exp, "")
}

func TestWatchAdd(t *testing.T) {
	s := New(logger)

	ch := s.Watch("/", Add)
	assert.Equal(t, 1, len(s.watches["/"]), "")

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/y", "c")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)

	expa := <-ch
	assert.Equal(t, Event{Add, 1, "/", "x"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Add, 3, "/", "y"}, expb, "")
}

func TestWatchRem(t *testing.T) {
	s := New(logger)

	ch := s.Watch("/", Rem)
	assert.Equal(t, 1, len(s.watches["/"]), "")

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/y", "c")
	mut4, _ := EncodeDel("/x")
	mut5, _ := EncodeDel("/y")
	mut6, _ := EncodeDel("/x")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)

	expa := <-ch
	assert.Equal(t, Event{Rem, 4, "/", "x"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Rem, 5, "/", "y"}, expb, "")
}

func TestWatchDelDirParents(t *testing.T) {
	s := New(logger)

	ch := s.Watch("/", Rem)
	assert.Equal(t, 1, len(s.watches["/"]), "")

	mut1, _ := EncodeSet("/x/y/z", "a")
	s.Apply(1, mut1)

	mut2, _ := EncodeDel("/x/y/z")
	s.Apply(2, mut2)

	expa := <-ch
	assert.Equal(t, Event{Rem, 2, "/", "x"}, expa, "")
}

func TestWatchSetDirParents(t *testing.T) {
	s := New(logger)

	ch := s.Watch("/x", Add)
	assert.Equal(t, 1, len(s.watches["/x"]), "")

	mut, _ := EncodeSet("/x/y/z", "a")
	s.Apply(1, mut)

	expa := <-ch
	assert.Equal(t, Event{Add, 1, "/x", "y"}, expa, "")
}
