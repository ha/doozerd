package store

import (
	"junta/assert"
	"bytes"
	"gob"
	"strconv"
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

func clearGetter(ev Event) Event {
	ev.Getter = nil
	return ev
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
		assert.Equal(t, ErrBadPath, err)
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
		gotk, gotv, gotc, keep, err := decode(m)
		assert.Equal(t, nil, err)
		assert.Equal(t, true, keep, "keep from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, expv, gotv, "value from " + m)
		assert.Equal(t, expc, gotc, "cas from " + m)
	}
}

func TestDecodeDel(t *testing.T) {
	for _, kcm := range DelKCMs {
		expk, expc, m := kcm[0], kcm[1], kcm[2]
		gotk, gotv, gotc, keep, err := decode(m)
		assert.Equal(t, nil, err)
		assert.Equal(t, false, keep, "keep from " + m)
		assert.Equal(t, expk, gotk, "key from " + m)
		assert.Equal(t, "", gotv, "value from " + m)
		assert.Equal(t, expc, gotc, "cas from " + m)
	}
}

func TestDecodeBadInstructions(t *testing.T) {
	for _, m := range BadInstructions {
		_, _, _, _, err := decode(m)
		assert.Equal(t, ErrBadPath, err)
	}
}

func TestDecodeBadMutations(t *testing.T) {
	for _, m := range BadMutations {
		_, _, _, _, err := decode(m)
		assert.Equal(t, ErrBadMutation, err)
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	v, cas := s.Get("/x")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, []string{""}, v)
}

func TestGet(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Sync(1)
	v, cas := s.Get("/x")
	assert.Equal(t, "1", cas)
	assert.Equal(t, []string{"a"}, v)
}

func TestGetDeleted(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(2, MustEncodeDel("/x", Clobber))
	s.Sync(2)
	v, cas := s.Get("/x")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, []string{""}, v)
}

func TestApplyInOrder(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s.Sync(2)
	v, cas := s.Get("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, []string{"b"}, v)
}

func TestGetSync(t *testing.T) {
	chV := make(chan []string)
	chCas := make(chan string)
	s := New()
	go func() {
		s.Sync(5)
		v, cas := s.Get("/x")
		chV <- v
		chCas <- cas
	}()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(2, MustEncodeSet("/x", "a", Clobber))
	s.Apply(3, MustEncodeSet("/x", "a", Clobber))
	s.Apply(4, MustEncodeSet("/x", "a", Clobber))
	s.Apply(5, MustEncodeSet("/x", "b", Clobber))
	s.Sync(5)
	assert.Equal(t, []string{"b"}, <-chV)
	assert.Equal(t, "5", <-chCas)
}

func TestGetSyncSeveral(t *testing.T) {
	chV := make(chan []string)
	chCas := make(chan string)
	s := New()
	go func() {
		s.Sync(0)
		v, cas := s.Get("/x")
		chV <- v
		chCas <- cas

		s.Sync(5)
		v, cas = s.Get("/x")
		chV <- v
		chCas <- cas

		s.Sync(0)
		v, cas = s.Get("/x")
		chV <- v
		chCas <- cas
	}()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(2, MustEncodeSet("/x", "a", Clobber))
	s.Apply(3, MustEncodeSet("/x", "a", Clobber))
	s.Apply(4, MustEncodeSet("/x", "a", Clobber))
	s.Apply(5, MustEncodeSet("/x", "b", Clobber))
	v := <-chV
	assert.Equal(t, 1, len(v))
	assert.T(t, "a" == v[0] || "b" == v[0])
	n, err := strconv.Atoi(<-chCas)
	assert.Equal(t, nil, err)
	assert.T(t, n >= 1)

	assert.Equal(t, []string{"b"}, <-chV)
	assert.Equal(t, "5", <-chCas)
	assert.Equal(t, []string{"b"}, <-chV)
	assert.Equal(t, "5", <-chCas)
}

func TestGetSyncExtra(t *testing.T) {
	chV := make(chan []string)
	chCas := make(chan string)
	s := New()

	go func() {
		s.Sync(0)
		v, cas := s.Get("/x")
		chV <- v
		chCas <- cas

		s.Sync(5)
		v, cas = s.Get("/x")
		chV <- v
		chCas <- cas

		s.Sync(0)
		v, cas = s.Get("/x")
		chV <- v
		chCas <- cas
	}()

	// Assert here to ensure correct ordering
	assert.Equal(t, []string{""}, <-chV)
	assert.Equal(t, Missing, <-chCas)

	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(2, MustEncodeSet("/x", "a", Clobber))
	s.Apply(3, MustEncodeSet("/x", "a", Clobber))
	s.Apply(4, MustEncodeSet("/x", "a", Clobber))
	// 5 is below
	s.Apply(6, MustEncodeSet("/x", "c", Clobber))
	s.Apply(7, MustEncodeSet("/x", "c", Clobber))
	s.Apply(8, MustEncodeSet("/x", "c", Clobber))
	// do 5 last
	s.Apply(5, MustEncodeSet("/x", "b", Clobber))

	v := <-chV
	assert.Equal(t, 1, len(v))
	assert.T(t, "b" == v[0] || "c" == v[0])
	n, err := strconv.Atoi(<-chCas)
	assert.Equal(t, nil, err)
	assert.T(t, n >= 5)

	v = <-chV
	assert.Equal(t, 1, len(v))
	assert.T(t, "b" == v[0] || "c" == v[0])
	n, err = strconv.Atoi(<-chCas)
	assert.Equal(t, nil, err)
	assert.T(t, n >= 5)
}

func TestApplyBadThenGood(t *testing.T) {
	s := New()
	s.Apply(1, "foo") // bad mutation
	s.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s.Sync(2)
	v, cas := s.Get("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, []string{"b"}, v)
}

func TestApplyOutOfOrder(t *testing.T) {
	s := New()
	s.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))

	s.Sync(2)
	v, cas := s.Get("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, []string{"b"}, v)
}

func TestApplyIgnoreDuplicate(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(1, MustEncodeSet("/x", "b", Clobber))
	s.Sync(1)
	v, cas := s.Get("/x")
	assert.Equal(t, "1", cas)
	assert.Equal(t, []string{"a"}, v)

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo))
}

func TestApplyIgnoreDuplicateOutOfOrder(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s.Apply(1, MustEncodeSet("/x", "c", Clobber))
	s.Sync(1)
	v, cas := s.Get("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, []string{"b"}, v)

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s.todo))
}

func TestGetWithDir(t *testing.T) {
	s := New()
	s.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s.Apply(2, MustEncodeSet("/y", "b", Clobber))
	s.Sync(2)
	dents, cas := s.Get("/")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, []string{"x", "y"}, dents)
}

func TestDirParents(t *testing.T) {
	s := New()

	s.Apply(1, MustEncodeSet("/x/y/z", "a", Clobber))
	s.Sync(1)

	dents, cas := s.Get("/")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, []string{"x"}, dents)

	dents, cas = s.Get("/x")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, []string{"y"}, dents)

	dents, cas = s.Get("/x/y")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, []string{"z"}, dents)

	v, cas := s.Get("/x/y/z")
	assert.Equal(t, "1", cas)
	assert.Equal(t, []string{"a"}, v)
}

func TestDelDirParents(t *testing.T) {
	s := New()

	s.Apply(1, MustEncodeSet("/x/y/z", "a", Clobber))

	s.Apply(2, MustEncodeDel("/x/y/z", Clobber))
	s.Sync(2)

	v, cas := s.Get("/")
	assert.Equal(t, Dir, cas)
	assert.Equal(t, []string{""}, v, "lookup /")

	v, cas = s.Get("/x")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, []string{""}, v, "lookup /x")

	v, cas = s.Get("/x/y")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, []string{""}, v, "lookup /x/y")

	v, cas = s.Get("/x/y/z")
	assert.Equal(t, Missing, cas)
	assert.Equal(t, []string{""}, v, "lookup /x/y/z")
}

func TestWatchSet(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", ch)

	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Sync(3)

	expa := clearGetter(<-ch)
	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil, nil}, expa)
	expb := clearGetter(<-ch)
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil, nil}, expb)
}

func TestWatchSetOutOfOrder(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", ch)

	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)

	s.Apply(2, mut2)
	s.Apply(1, mut1)
	s.Apply(3, mut3)
	s.Sync(3)

	expa := clearGetter(<-ch)
	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil, nil}, expa)
	expb := clearGetter(<-ch)
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil, nil}, expb)
}

func TestWatchDel(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x", ch)

	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	mut4 := MustEncodeDel("/x", Clobber)
	mut5 := MustEncodeDel("/y", Clobber)
	mut6 := MustEncodeDel("/x", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)
	s.Sync(6)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil, nil}, clearGetter(<-ch))
}

func TestWatchAdd(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/*", ch)

	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Sync(3)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil, nil}, clearGetter(<-ch))
}

func TestWatchAddOutOfOrder(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/*", ch)

	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)

	s.Apply(3, mut3)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Sync(2)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil, nil}, clearGetter(<-ch))
}

func TestWatchRem(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/*", ch)

	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	mut4 := MustEncodeDel("/x", Clobber)
	mut5 := MustEncodeDel("/y", Clobber)
	mut6 := MustEncodeDel("/x", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)
	s.Sync(6)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil, nil}, clearGetter(<-ch))

	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{5, "/y", "", Missing, mut5, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil, nil}, clearGetter(<-ch))
}

func TestWatchSetDirParents(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/x/**", ch)

	mut1 := MustEncodeSet("/x/y/z", "a", Clobber)
	s.Apply(1, mut1)
	s.Sync(1)

	assert.Equal(t, Event{1, "/x/y/z", "a", "1", mut1, nil, nil}, clearGetter(<-ch))
}

func TestWatchDelDirParents(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/**", ch)

	mut1 := MustEncodeSet("/x/y/z", "a", Clobber)
	s.Apply(1, mut1)

	mut2 := MustEncodeDel("/x/y/z", Clobber)
	s.Apply(2, mut2)
	s.Sync(2)

	assert.Equal(t, Event{1, "/x/y/z", "a", "1", mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x/y/z", "", Missing, mut2, nil, nil}, clearGetter(<-ch))
}

func TestWatchApply(t *testing.T) {
	s := New()

	ch := make(chan Event)
	s.Watch("/**", ch)

	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	mut4 := MustEncodeDel("/x", Clobber)
	mut5 := MustEncodeDel("/y", Clobber)
	mut6 := MustEncodeDel("/x", Clobber)
	s.Apply(1, mut1)
	s.Apply(2, mut2)
	s.Apply(3, mut3)
	s.Apply(4, mut4)
	s.Apply(5, mut5)
	s.Apply(6, mut6)
	s.Sync(6)

	assert.Equal(t, Event{1, "/x", "a", "1", mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", "2", mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", "3", mut3, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{5, "/y", "", Missing, mut5, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil, nil}, clearGetter(<-ch))
}

func TestSnapshotApply(t *testing.T) {
	s1 := New()
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	s1.Sync(2)
	seqn, snap := s1.Snapshot()
	assert.Equal(t, uint64(2), seqn)

	s2 := New()
	s2.Apply(1, snap)
	s2.Sync(1)

	v, cas := s2.Get("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, []string{"b"}, v)
}

func TestSnapshotBad(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	gob.NewEncoder(buf).Encode(uint64(1))
	seqnPart := buf.String()

	buf = bytes.NewBuffer([]byte{})
	gob.NewEncoder(buf).Encode(emptyDir)
	valPart := buf.String()
	valPart = valPart[0:len(valPart)/2]


	st := New()
	st.Apply(1, seqnPart+valPart)
	st.Sync(1)

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(st.todo))
}

func TestSnapshotSeqn(t *testing.T) {
	s1 := New()
	s1.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s1.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s1.Sync(2)
	seqn, snap := s1.Snapshot()
	assert.Equal(t, uint64(2), seqn)

	s2 := New()
	s2.Apply(1, snap)
	s2.Sync(1)
	v, cas := s2.Get("/x")
	assert.Equal(t, "2", cas, "snap")
	assert.Equal(t, []string{"b"}, v, "snap")

	s2.Apply(1, MustEncodeSet("/x", "x", Clobber))
	s2.Sync(1)
	v, cas = s2.Get("/x")
	assert.Equal(t, "2", cas, "x")
	assert.Equal(t, []string{"b"}, v, "x")

	s2.Apply(2, MustEncodeSet("/x", "y", Clobber))
	s2.Sync(2)
	v, cas = s2.Get("/x")
	assert.Equal(t, "2", cas, "y")
	assert.Equal(t, []string{"b"}, v, "y")

	s2.Apply(3, MustEncodeSet("/x", "z", Clobber))
	s2.Sync(3)
	v, cas = s2.Get("/x")
	assert.Equal(t, "3", cas, "z")
	assert.Equal(t, []string{"z"}, v, "z")
}

func TestSnapshotLeak(t *testing.T) {
	s1 := New()
	s1.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s1.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s1.Sync(2)
	seqn, snap := s1.Snapshot()
	assert.Equal(t, uint64(2), seqn)

	s2 := New()

	s2.Apply(2, MustEncodeSet("/x", "c", Clobber))
	s2.Apply(1, snap)
	s2.Sync(1)

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s2.todo))
}

func TestSnapshotOutOfOrder(t *testing.T) {
	s1 := New()
	s1.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s1.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s1.Sync(2)
	seqn, snap := s1.Snapshot()
	assert.Equal(t, uint64(2), seqn)

	s2 := New()

	s2.Apply(2, MustEncodeSet("/x", "c", Clobber))
	s2.Apply(3, MustEncodeSet("/x", "c", Clobber))
	s2.Apply(1, snap)
	s2.Sync(3)

	body, cas := s2.Get("/x")
	assert.Equal(t, []string{"c"}, body)
	assert.Equal(t, "3", cas)

	// check that we aren't leaking memory
	assert.Equal(t, 0, len(s2.todo))
}

func TestSnapshotSync(t *testing.T) {
	seqnCh := make(chan uint64)
	snapCh := make(chan string)
	s1 := New()
	go func() {
		s1.Sync(2)
		seqn, snap := s1.Snapshot()
		seqnCh <- seqn
		snapCh <- snap
	}()
	s1.Apply(1, MustEncodeSet("/x", "a", Clobber))
	s1.Apply(2, MustEncodeSet("/x", "b", Clobber))
	s1.Sync(2)
	seqn := <-seqnCh
	assert.Equal(t, uint64(2), seqn)
	snap := <-snapCh

	s2 := New()
	s2.Apply(1, snap)
	s2.Sync(1)

	v, cas := s2.Get("/x")
	assert.Equal(t, "2", cas)
	assert.Equal(t, []string{"b"}, v)
}

func TestStoreWaitWorks(t *testing.T) {
	st := New()
	mut := MustEncodeSet("/x", "a", Clobber)

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
	evCh := make(chan Event)
	statusCh := make(chan Event)

	st.Watch("/**", evCh)

	st.Apply(1, MustEncodeSet("/x", "a", Clobber))
	st.Apply(2, MustEncodeSet("/x", "b", Clobber))
	st.Sync(2)

	st.Wait(1, statusCh)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, ErrTooLate, got.Err)
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

	st.Watch("/**", evCh)

	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, ErrBadMutation, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitBadInstruction(t *testing.T) {
	st := New()
	mut := BadInstructions[0]

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st.Watch("/**", evCh)

	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, ErrBadPath, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitCasMatchAdd(t *testing.T) {
	mut := MustEncodeSet("/a", "foo", Missing)

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.Watch("/**", evCh)
	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitCasMatchReplace(t *testing.T) {
	mut1 := MustEncodeSet("/a", "foo", Clobber)
	mut2 := MustEncodeSet("/a", "foo", "1")

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.Watch("/**", evCh)
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
	mut := MustEncodeSet("/a", "foo", "123")

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.Watch("/**", evCh)
	st.Wait(1, statusCh)
	st.Apply(1, mut)

	got := <-statusCh
	assert.Equal(t, uint64(1), got.Seqn)
	assert.Equal(t, ErrCasMismatch, got.Err)
	assert.Equal(t, mut, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
}

func TestStoreWaitCasMismatchReplace(t *testing.T) {
	mut1 := MustEncodeSet("/a", "foo", Clobber)
	mut2 := MustEncodeSet("/a", "foo", "123")

	evCh := make(chan Event)
	statusCh := make(chan Event)

	st := New()

	st.Watch("/**", evCh)
	st.Wait(2, statusCh)
	st.Apply(1, mut1)
	st.Apply(2, mut2)

	got := <-statusCh
	assert.Equal(t, uint64(2), got.Seqn)
	assert.Equal(t, ErrCasMismatch, got.Err)
	assert.Equal(t, mut2, got.Mut)

	assert.Equal(t, uint64(1), (<-evCh).Seqn)
	assert.Equal(t, uint64(2), (<-evCh).Seqn)
}

func TestSyncPathFuture(t *testing.T) {
	st := New()

	go func() {
		st.Apply(1, MustEncodeSet("/x", "a", ""))
		st.Apply(2, MustEncodeSet("/y", "b", ""))
		st.Apply(3, MustEncodeSet("/y", "c", ""))
		st.Apply(4, MustEncodeSet("/z", "d", ""))
	}()

	g := st.SyncPath("/y")
	got := GetString(g, "/y")
	assert.Equal(t, "b", got)
}

func TestSyncPathImmediate(t *testing.T) {
	st := New()

	st.Apply(1, MustEncodeSet("/x", "a", ""))
	st.Apply(2, MustEncodeSet("/y", "b", ""))

	g := st.SyncPath("/y")
	got := GetString(g, "/y")
	assert.Equal(t, "b", got)
}
