package store

import (
	"junta/assert"
	"bytes"
	"os"
	"testing"
)

var SetKVCMs = [][]string{
	[]string{"/", "a", Clobber, ":/=a"},
	[]string{"/x", "a", Clobber, ":/x=a"},
	[]string{"/x", "a=b", Clobber, ":/x=a=b"},
	[]string{"/x", "a b", Clobber, ":/x=a b"},
	[]string{"/", "a", Missing, "0:/=a"},
	[]string{"/", "a", "123", "123:/=a"},
}

var DelKCMs = [][]string{
	[]string{"/", Clobber, ":/"},
	[]string{"/x", Clobber, ":/x"},
	[]string{"/", Missing, "0:/"},
	[]string{"/", "123", "123:/"},
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

var BadInstructions = []string{
	":",
	":x",
	":/x y",
	":=",
	":x=",
	":/x y=",
}

// Anything without a colon is a bad mutation because
// it is missing cas.
var BadMutations = []string{
	"",
	"x",
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
		assert.Equal(t, BadPathError, err)
	}
}

func TestCheckGoodPaths(t *testing.T) {
	for _, k := range GoodPaths {
		err := checkPath(k)
		assert.Equal(t, nil, err, k)
	}
}

func TestEncodeSet(t *testing.T) {
	for _, kvcm := range SetKVCMs {
		k, v, c, exp := kvcm[0], kvcm[1], kvcm[2], kvcm[3]
		got, err := EncodeSet(k, v, c)
		assert.Equal(t, nil, err)
		assert.Equal(t, exp, got)
	}
}

func TestEncodeDel(t *testing.T) {
	for _, kcm := range DelKCMs {
		k, c, exp := kcm[0], kcm[1], kcm[2]
		got, err := EncodeDel(k, c)
		assert.Equal(t, nil, err)
		assert.Equal(t, exp, got)
	}
}

func TestDecodeSet(t *testing.T) {
	for _, kvcm := range SetKVCMs {
		expk, expv, expc, m := kvcm[0], kvcm[1], kvcm[2], kvcm[3]
		op, gotk, gotv, gotc, err := decode(m)
		assert.Equal(t, nil, err)
		assert.Equal(t, Set, op, "op from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, expv, gotv, "value from " + m)
		assert.Equal(t, expc, gotc, "cas from " + m)
	}
}

func TestDecodeDel(t *testing.T) {
	for _, kcm := range DelKCMs {
		expk, expc, m := kcm[0], kcm[1], kcm[2]
		op, gotk, gotv, gotc, err := decode(m)
		assert.Equal(t, nil, err)
		assert.Equal(t, Del, op, "op from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, "", gotv, "value from " + m)
		assert.Equal(t, expc, gotc, "cas from " + m)
	}
}

func TestDecodeBadInstructions(t *testing.T) {
	for _, m := range BadInstructions {
		_, _, _, _, err := decode(m)
		assert.Equal(t, BadPathError, err)
	}
}

func TestDecodeBadMutations(t *testing.T) {
	for _, m := range BadMutations {
		_, _, _, _, err := decode(m)
		assert.Equal(t, BadMutationError, err)
	}
}

func TestLookupMissing(t *testing.T) {
	s := New()
	v, cas := s.Lookup("/x")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, "", v)
}

func TestLookup(t *testing.T) {
	s := New()
	mut, _ := EncodeSet("/x", "a", Clobber)
	s.Apply(1, mut)
	s.Sync(1)
	v, cas := s.Lookup("/x")
	assert.Equal(t, "1", cas)
	assert.Equal(t, "a", v)
}

func TestLookupDeleted(t *testing.T) {
	s := New()
	mut, _ := EncodeSet("/x", "a", Clobber)
	s.Apply(1, mut)
	mut, _ = EncodeDel("/x", Clobber)
	s.Apply(2, mut)
	s.Sync(2)
	v, cas := s.Lookup("/x")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, "", v)
}

func TestApplyInOrder(t *testing.T) {
	s := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Sync(2)
	v, cas := s.Lookup("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, "b", v)
}

func TestLookupSync(t *testing.T) {
	chV := make(chan string)
	chCas := make(chan string)
	s := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	go func() {
		v, cas := s.LookupSync("/x", 5)
		chV <- v
		chCas <- cas
	}()
	s.Apply(1, mut1)
	s.Apply(2, mut1)
	s.Apply(3, mut1)
	s.Apply(4, mut1)
	s.Apply(5, mut2)
	s.Sync(5)
	assert.Equal(t, "b", <-chV)
	assert.Equal(t, "5", <-chCas)
}

func TestLookupSyncSeveral(t *testing.T) {
	chV := make(chan string)
	chCas := make(chan string)
	s := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	go func() {
		v, cas := s.LookupSync("/x", 0)
		chV <- v
		chCas <- cas

		v, cas = s.LookupSync("/x", 5)
		chV <- v
		chCas <- cas

		v, cas = s.LookupSync("/x", 0)
		chV <- v
		chCas <- cas
	}()
	s.Apply(1, mut1)
	s.Sync(1)
	s.Apply(2, mut1)
	s.Apply(3, mut1)
	s.Apply(4, mut1)
	s.Apply(5, mut2)
	s.Sync(5)
	assert.Equal(t, "a", <-chV)
	assert.Equal(t, "1", <-chCas)
	assert.Equal(t, "b", <-chV)
	assert.Equal(t, "5", <-chCas)
	assert.Equal(t, "b", <-chV)
	assert.Equal(t, "5", <-chCas)
}

func TestLookupSyncExtra(t *testing.T) {
	chV := make(chan string)
	chCas := make(chan string)
	s := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/x", "c", Clobber)

	go func() {
		v, cas := s.LookupSync("/x", 0)
		chV <- v
		chCas <- cas

		v, cas = s.LookupSync("/x", 5)
		chV <- v
		chCas <- cas

		v, cas = s.LookupSync("/x", 0)
		chV <- v
		chCas <- cas
	}()

	// Assert here to ensure correct ordering
	assert.Equal(t, "", <-chV)
	assert.Equal(t, Missing, <-chCas)

	s.Apply(1, mut1)
	s.Apply(2, mut1)
	s.Apply(3, mut1)
	s.Apply(4, mut1)
	// 5 is below
	s.Apply(6, mut3)
	s.Apply(7, mut3)
	s.Apply(8, mut3)
	// do 5 last
	s.Apply(5, mut2)

	assert.Equal(t, "c", <-chV)
	assert.Equal(t, "8", <-chCas)
	assert.Equal(t, "c", <-chV)
	assert.Equal(t, "8", <-chCas)
}

func TestApplyBadThenGood(t *testing.T) {
	s := New()
	mut1 := "foo" // bad mutation
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Sync(2)
	v, cas := s.Lookup("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, "b", v)
}

func TestApplyOutOfOrder(t *testing.T) {
	s := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s.Apply(2, mut2)
	s.Apply(1, mut1)
	s.Sync(1)
	v, cas := s.LookupSync("/x", 2)
	assert.Equal(t, "2", cas)
	assert.Equal(t, "b", v)
}

func TestApplyIgnoreDuplicate(t *testing.T) {
	s := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s.Apply(1, mut1)
	s.Apply(1, mut2)
	s.Sync(1)
	v, cas := s.Lookup("/x")
	assert.Equal(t, "1", cas)
	assert.Equal(t, "a", v)

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo))
}

func TestApplyIgnoreDuplicateOutOfOrder(t *testing.T) {
	s := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/x", "c", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(1, mut3)
	s.Sync(1)
	v, cas := s.Lookup("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, "b", v)

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo))
}

func TestGetDir(t *testing.T) {
	s := New()

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/y", "b", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Sync(2)

	v, cas := s.Lookup("/")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, "x\ny\n", v)
}

func TestDirParents(t *testing.T) {
	s := New()

	mut1, _ := EncodeSet("/x/y/z", "a", Clobber)
	s.Apply(1, mut1)
	s.Sync(1)

	v, cas := s.Lookup("/")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, "x\n", v)

	v, cas = s.Lookup("/x")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, "y\n", v)

	v, cas = s.Lookup("/x/y")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, "z\n", v)

	v, cas = s.Lookup("/x/y/z")
	assert.Equal(t, "1", cas)
	assert.Equal(t, "a", v)
}

func TestDelDirParents(t *testing.T) {
	s := New()

	mut1, _ := EncodeSet("/x/y/z", "a", Clobber)
	s.Apply(1, mut1)

	mut2, _ := EncodeDel("/x/y/z", Clobber)
	s.Apply(2, mut2)
	s.Sync(2)

	v, cas := s.Lookup("/")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, "", v, "lookup /")

	v, cas = s.Lookup("/x")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, "", v, "lookup /x")

	v, cas = s.Lookup("/x/y")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, "", v, "lookup /x/y")

	v, cas = s.Lookup("/x/y/z")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, "", v, "lookup /x/y/z")
}

func TestWatchSet(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", ch)

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/y", "c", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Sync(3)

	expa := <-ch
	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil}, expa)
	expb := <-ch
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil}, expb)
}

func TestWatchSetOutOfOrder(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", ch)

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/y", "c", Clobber)

	s.Apply(2, mut2)
	s.Apply(1, mut1)
	s.Apply(3, mut3)
	s.Sync(3)

	expa := <-ch
	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil}, expa)
	expb := <-ch
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil}, expb)
}

func TestWatchDel(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", ch)

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/y", "c", Clobber)
	mut4, _ := EncodeDel("/x", Clobber)
	mut5, _ := EncodeDel("/y", Clobber)
	mut6, _ := EncodeDel("/x", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)
	s.Sync(6)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil}, <-ch)
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil}, <-ch)
	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil}, <-ch)
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil}, <-ch)
}

func TestWatchAdd(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/*", ch)

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/y", "c", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Sync(3)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil}, <-ch)
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil}, <-ch)
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil}, <-ch)
}

func TestWatchAddOutOfOrder(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/*", ch)

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/y", "c", Clobber)

	s.Apply(3, mut3)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Sync(2)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil}, <-ch)
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil}, <-ch)
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil}, <-ch)
}

func TestWatchRem(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/*", ch)

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/y", "c", Clobber)
	mut4, _ := EncodeDel("/x", Clobber)
	mut5, _ := EncodeDel("/y", Clobber)
	mut6, _ := EncodeDel("/x", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)
	s.Sync(6)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil}, <-ch)
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil}, <-ch)
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil}, <-ch)

	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil}, <-ch)
	assert.Equal(t, Event{5, "/y", "", Missing, mut5, nil}, <-ch)
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil}, <-ch)
}

func TestWatchSetDirParents(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x/**", ch)

	mut1, _ := EncodeSet("/x/y/z", "a", Clobber)
	s.Apply(1, mut1)
	s.Sync(1)

	assert.Equal(t, Event{1, "/x/y/z", "a", "1", mut1, nil}, <-ch)
}

func TestWatchDelDirParents(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/**", ch)

	mut1, _ := EncodeSet("/x/y/z", "a", Clobber)
	s.Apply(1, mut1)

	mut2, _ := EncodeDel("/x/y/z", Clobber)
	s.Apply(2, mut2)
	s.Sync(2)

	assert.Equal(t, Event{1, "/x/y/z", "a", "1", mut1, nil}, <-ch)
	assert.Equal(t, Event{2, "/x/y/z", "", Missing, mut2, nil}, <-ch)
}

func TestWatchApply(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/**", ch)

	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	mut3, _ := EncodeSet("/y", "c", Clobber)
	mut4, _ := EncodeDel("/x", Clobber)
	mut5, _ := EncodeDel("/y", Clobber)
	mut6, _ := EncodeDel("/x", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)
	s.Sync(6)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil}, <-ch)
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil}, <-ch)
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil}, <-ch)
	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil}, <-ch)
	assert.Equal(t, Event{5, "/y", "", Missing, mut5, nil}, <-ch)
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil}, <-ch)
}

func TestSnapshotApply(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	s1 := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	s1.Sync(2)
	err := s1.SnapshotSync(1, buf)
	assert.Equal(t, nil, err)

	s2 := New()
	s2.Apply(1, buf.String())
	s2.Sync(1)

	v, cas := s2.Lookup("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, "b", v)
}

func TestSnapshotSeqn(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	s1 := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	s1.Sync(2)
	err := s1.SnapshotSync(1, buf)
	assert.Equal(t, nil, err)

	s2 := New()
	s2.Apply(1, buf.String())
	s2.Sync(1)
	v, cas := s2.Lookup("/x")
	assert.Equal(t, "2", cas, "snap")
	assert.Equal(t, "b", v, "snap")

	mutx, _ := EncodeSet("/x", "x", Clobber)
	s2.Apply(1, mutx)
	s2.Sync(1)
	v, cas = s2.LookupSync("/x", 1)
	assert.Equal(t, "2", cas, "x")
	assert.Equal(t, "b", v, "x")

	muty, _ := EncodeSet("/x", "y", Clobber)
	s2.Apply(2, muty)
	s2.Sync(2)
	v, cas = s2.LookupSync("/x", 2)
	assert.Equal(t, "2", cas, "y")
	assert.Equal(t, "b", v, "y")

	mutz, _ := EncodeSet("/x", "z", Clobber)
	s2.Apply(3, mutz)
	s2.Sync(3)
	v, cas = s2.LookupSync("/x", 3)
	assert.Equal(t, "3", cas, "z")
	assert.Equal(t, "z", v, "z")
}

func TestSnapshotLeak(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	s1 := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	s1.Sync(2)
	err := s1.SnapshotSync(1, buf)
	assert.Equal(t, nil, err)

	s2 := New()

	mut3, _ := EncodeSet("/x", "c", Clobber)
	s2.Apply(2, mut3)
	s2.Apply(3, mut3)
	s2.Apply(1, buf.String())
	s2.Sync(1)

	// check that we aren't leaking memory
	s2.Wait(3, make(chan Event))
	assert.Equal(t, 0, len(s2.todo))
}

func TestSnapshotSync(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	ch := make(chan os.Error)
	s1 := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	go func() {
		ch <- s1.SnapshotSync(2, buf)
	}()
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	s1.Sync(2)
	err := <-ch
	assert.Equal(t, nil, err)

	s2 := New()
	s2.Apply(1, buf.String())
	s2.Sync(1)

	v, cas := s2.Lookup("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, "b", v)
}

func TestStoreWaitWorks(t *testing.T) {
	st := New()
	mut, _ := EncodeSet("/x", "a", Clobber)

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st.Watch("/**", evCh)

	st.Wait(1, statusCh)
	st.Apply(1, mut)
	st.Sync(1)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut, got.Mut)
	assert.Equal(t, 0, len(st.todo))

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitOutOfOrder(t *testing.T) {
	st := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	evCh := make(chan Event)
	statusCh := make(chan Event)

	st.WatchApply(evCh)

	st.Apply(1, mut1)
	st.Apply(2, mut2)
	st.Sync(2)

	st.Wait(1, statusCh)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, TooLateError, got.Err)
	assert.Equal(t, "", got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
	assert.Equal(t, uint64(2), (<-evCh).Seqn)
}

func TestStoreWaitBadMutation(t *testing.T) {
	st := New()
	mut := BadMutations[0]

	evCh := make(chan Event)
	t.Logf("evCh=%v", evCh)
	statusCh := make(chan Event)

	st.WatchApply(evCh)

	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, BadMutationError, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitBadInstruction(t *testing.T) {
	st := New()
	mut := BadInstructions[0]

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st.WatchApply(evCh)

	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, BadPathError, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitCasMatchAdd(t *testing.T) {
	mut, _ := EncodeSet("/a", "foo", Missing)

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.WatchApply(evCh)
	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitCasMatchReplace(t *testing.T) {
	mut1, _ := EncodeSet("/a", "foo", Clobber)
	mut2, _ := EncodeSet("/a", "foo", "1")

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.WatchApply(evCh)
	st.Wait(2, statusCh)
	st.Apply(1, mut1)
	st.Apply(2, mut2)

	got := <-statusCh
	assert.Equal(t, uint64(2), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut2, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
	assert.Equal(t, uint64(2), (<-evCh).Seqn)
}

func TestStoreWaitCasMismatchMissing(t *testing.T) {
	mut, _ := EncodeSet("/a", "foo", "123")

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.WatchApply(evCh)
	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, CasMismatchError, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitCasMismatchReplace(t *testing.T) {
	mut1, _ := EncodeSet("/a", "foo", Clobber)
	mut2, _ := EncodeSet("/a", "foo", "123")

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.WatchApply(evCh)
	st.Wait(2, statusCh)
	st.Apply(1, mut1)
	st.Apply(2, mut2)

	got := <-statusCh
	assert.Equal(t, uint64(2), got.Seqn)
	assert.Equal(t, CasMismatchError, got.Err)
	assert.Equal(t, mut2, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
	assert.Equal(t, uint64(2), (<-evCh).Seqn)
}
