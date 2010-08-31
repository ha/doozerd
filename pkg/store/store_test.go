package store

import (
	"junta/assert"
	"bytes"
	"os"
	"testing"
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
		assert.Equal(t, BadPathError, err, "")
	}
}

func TestCheckGoodPaths(t *testing.T) {
	for _, k := range GoodPaths {
		err := checkPath(k)
		assert.Equal(t, nil, err, k)
	}
}

func TestEncodeSet(t *testing.T) {
	for _, kvm := range SetKVMs {
		k, v, exp := kvm[0], kvm[1], kvm[2]
		got, err := EncodeSet(k, v)
		assert.Equal(t, nil, err, "")
		assert.Equal(t, exp, got, "")
	}
}

func TestEncodeDel(t *testing.T) {
	for _, kvm := range DelKVMs {
		k, exp := kvm[0], kvm[1]
		got, err := EncodeDel(k)
		assert.Equal(t, nil, err, "")
		assert.Equal(t, exp, got, "")
	}
}

func TestDecodeSet(t *testing.T) {
	for _, kvm := range SetKVMs {
		expk, expv, m := kvm[0], kvm[1], kvm[2]
		op, gotk, gotv, err := decode(m)
		assert.Equal(t, nil, err, "")
		assert.Equal(t, Set, op, "op from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, expv, gotv, "value from " + m)
	}
}

func TestDecodeDel(t *testing.T) {
	for _, kvm := range DelKVMs {
		expk, m := kvm[0], kvm[1]
		op, gotk, gotv, err := decode(m)
		assert.Equal(t, nil, err, "")
		assert.Equal(t, Del, op, "op from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, "", gotv, "value from " + m)
	}
}

func TestDecodeBadMutations(t *testing.T) {
	for _, m := range BadMutations {
		_, _, _, err := decode(m)
		assert.Equal(t, BadPathError, err, "")
	}
}

func TestLookupMissing(t *testing.T) {
	s := New()
	v, ok := s.Lookup("/x")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "")
}

func TestLookup(t *testing.T) {
	s := New()
	mut, _ := EncodeSet("/x", "a")
	s.Apply(1, mut)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "a", v, "")
}

func TestLookupDeleted(t *testing.T) {
	s := New()
	mut, _ := EncodeSet("/x", "a")
	s.Apply(1, mut)
	mut, _ = EncodeDel("/x")
	s.Apply(2, mut)
	v, ok := s.Lookup("/x")
	assert.Equal(t, false, ok, "")
	assert.Equal(t, "", v, "")
}

func TestApplyInOrder(t *testing.T) {
	s := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestLookupSync(t *testing.T) {
	chv := make(chan string)
	chok := make(chan bool)
	s := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	go func() {
		v, ok := s.LookupSync("/x", 5)
		chv <- v
		chok <- ok
	}()
	s.Apply(1, mut1)
	s.Apply(2, mut1)
	s.Apply(3, mut1)
	s.Apply(4, mut1)
	s.Apply(5, mut2)
	assert.Equal(t, "b", <-chv, "")
	assert.Equal(t, true, <-chok, "")
}

func TestLookupSyncSeveral(t *testing.T) {
	chv := make(chan string)
	chok := make(chan bool)
	s := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	go func() {
		v, ok := s.LookupSync("/x", 0)
		chv <- v
		chok <- ok

		v, ok = s.LookupSync("/x", 5)
		chv <- v
		chok <- ok

		v, ok = s.LookupSync("/x", 0)
		chv <- v
		chok <- ok
	}()
	s.Apply(1, mut1)
	s.Apply(2, mut1)
	s.Apply(3, mut1)
	s.Apply(4, mut1)
	s.Apply(5, mut2)
	assert.Equal(t, "a", <-chv, "")
	assert.Equal(t, true, <-chok, "")
	assert.Equal(t, "b", <-chv, "")
	assert.Equal(t, true, <-chok, "")
	assert.Equal(t, "b", <-chv, "")
	assert.Equal(t, true, <-chok, "")
}

func TestLookupSyncExtra(t *testing.T) {
	chv := make(chan string)
	chok := make(chan bool)
	s := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/x", "c")
	go func() {
		v, ok := s.LookupSync("/x", 0)
		chv <- v
		chok <- ok

		v, ok = s.LookupSync("/x", 5)
		chv <- v
		chok <- ok

		v, ok = s.LookupSync("/x", 0)
		chv <- v
		chok <- ok
	}()
	s.Apply(1, mut1)
	s.Apply(2, mut1)
	s.Apply(3, mut1)
	s.Apply(4, mut1)

	s.Apply(6, mut3)
	s.Apply(7, mut3)
	s.Apply(8, mut3)

	s.Apply(5, mut2)
	assert.Equal(t, "a", <-chv, "")
	assert.Equal(t, true, <-chok, "")
	assert.Equal(t, "c", <-chv, "")
	assert.Equal(t, true, <-chok, "")
	assert.Equal(t, "c", <-chv, "")
	assert.Equal(t, true, <-chok, "")
}

func TestApplyBadThenGood(t *testing.T) {
	s := New()
	mut1 := "foo" // bad mutation
	mut2, _ := EncodeSet("/x", "b")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestApplyOutOfOrder(t *testing.T) {
	s := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s.Apply(2, mut2)
	s.Apply(1, mut1)
	v, ok := s.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestApplyIgnoreDuplicate(t *testing.T) {
	s := New()
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
	s := New()
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
	s := New()

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/y", "b")
	s.Apply(1, mut1)
	s.Apply(2, mut2)

	v, ok := s.Lookup("/")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "x\ny\n", v, "")
}

func TestDirParents(t *testing.T) {
	s := New()

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
	s := New()

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
	s := New()

	ch := make(chan Event)
	s.Watch("/x", Set, ch)
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

func TestWatchSetOutOfOrder(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", Set, ch)
	assert.Equal(t, 1, len(s.watches["/x"]), "")

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/y", "c")
	s.Apply(2, mut2)
	s.Apply(1, mut1)
	s.Apply(3, mut3)

	expa := <-ch
	assert.Equal(t, Event{Set, 1, "/x", "a"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Set, 2, "/x", "b"}, expb, "")
}

func TestWatchDel(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", Del, ch)
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
	s := New()

	ch := make(chan Event)
	s.Watch("/", Add, ch)
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

func TestWatchAddOutOfOrder(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/", Add, ch)
	assert.Equal(t, 1, len(s.watches["/"]), "")

	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	mut3, _ := EncodeSet("/y", "c")
	s.Apply(3, mut3)
	s.Apply(1, mut1)
	s.Apply(2, mut2)

	expa := <-ch
	assert.Equal(t, Event{Add, 1, "/", "x"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Add, 3, "/", "y"}, expb, "")
}

func TestWatchAddSubdir(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/a", Add, ch)
	assert.Equal(t, 1, len(s.watches["/a"]), "")

	mut1, _ := EncodeSet("/a/x", "a")
	mut2, _ := EncodeSet("/a/x", "b")
	mut3, _ := EncodeSet("/a/y", "c")
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)

	expa := <-ch
	assert.Equal(t, Event{Add, 1, "/a", "x"}, expa, "")
	expb := <-ch
	assert.Equal(t, Event{Add, 3, "/a", "y"}, expb, "")
}

func TestWatchRem(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/", Rem, ch)
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
	s := New()

	ch := make(chan Event)
	s.Watch("/", Rem, ch)
	assert.Equal(t, 1, len(s.watches["/"]), "")

	mut1, _ := EncodeSet("/x/y/z", "a")
	s.Apply(1, mut1)

	mut2, _ := EncodeDel("/x/y/z")
	s.Apply(2, mut2)

	expa := <-ch
	assert.Equal(t, Event{Rem, 2, "/", "x"}, expa, "")
}

func TestWatchSetDirParents(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", Add, ch)
	assert.Equal(t, 1, len(s.watches["/x"]), "")

	mut, _ := EncodeSet("/x/y/z", "a")
	s.Apply(1, mut)

	expa := <-ch
	assert.Equal(t, Event{Add, 1, "/x", "y"}, expa, "")
}

func TestWatchApply(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", Del, ch)
	s.WatchApply(ch)
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

	assert.Equal(t, Event{Apply, 1, "", ""}, <-ch, "")
	assert.Equal(t, Event{Apply, 2, "", ""}, <-ch, "")
	assert.Equal(t, Event{Apply, 3, "", ""}, <-ch, "")
	assert.Equal(t, Event{Del, 4, "/x", ""}, <-ch, "")
	assert.Equal(t, Event{Apply, 4, "", ""}, <-ch, "")
	assert.Equal(t, Event{Apply, 5, "", ""}, <-ch, "")
	assert.Equal(t, Event{Apply, 6, "", ""}, <-ch, "")
}

func TestSnapshotApply(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	s1 := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	err := s1.SnapshotSync(1, buf)
	assert.Equal(t, nil, err, "")

	s2 := New()
	s2.Apply(1, buf.String())

	v, ok := s2.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}

func TestSnapshotSeqn(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	s1 := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	err := s1.SnapshotSync(1, buf)
	assert.Equal(t, nil, err, "")

	s2 := New()
	s2.Apply(1, buf.String())
	v, ok := s2.Lookup("/x")
	assert.Equal(t, true, ok, "snap")
	assert.Equal(t, "b", v, "snap")

	mutx, _ := EncodeSet("/x", "x")
	s2.Apply(1, mutx)
	v, ok = s2.LookupSync("/x", 1)
	assert.Equal(t, true, ok, "x")
	assert.Equal(t, "b", v, "x")

	muty, _ := EncodeSet("/x", "y")
	s2.Apply(2, muty)
	v, ok = s2.LookupSync("/x", 2)
	assert.Equal(t, true, ok, "y")
	assert.Equal(t, "b", v, "y")

	mutz, _ := EncodeSet("/x", "z")
	s2.Apply(3, mutz)
	v, ok = s2.LookupSync("/x", 3)
	assert.Equal(t, true, ok, "z")
	assert.Equal(t, "z", v, "z")
}

func TestSnapshotLeak(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	s1 := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	err := s1.SnapshotSync(1, buf)
	assert.Equal(t, nil, err, "")

	s2 := New()

	mut3, _ := EncodeSet("/x", "c")
	s2.Apply(2, mut3)
	s2.Apply(3, mut3)

	s2.Apply(1, buf.String())

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s2.todo), "")
}

func TestSnapshotSync(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	ch := make(chan os.Error)
	s1 := New()
	mut1, _ := EncodeSet("/x", "a")
	mut2, _ := EncodeSet("/x", "b")
	go func() {
		ch <- s1.SnapshotSync(2, buf)
	}()
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	err := <-ch
	assert.Equal(t, nil, err, "")

	s2 := New()
	s2.Apply(1, buf.String())

	v, ok := s2.Lookup("/x")
	assert.Equal(t, true, ok, "")
	assert.Equal(t, "b", v, "")
}
