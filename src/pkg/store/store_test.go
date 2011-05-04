package store

import (
	"github.com/bmizerany/assert"
	"sort"
	"testing"
)

type kvcm struct {
	k string
	v string
	r int64
	m string
}

var SetKVRM = []kvcm{
	{"/", "a", Clobber, "-1:/=a"},
	{"/x", "a", Clobber, "-1:/x=a"},
	{"/x", "a=b", Clobber, "-1:/x=a=b"},
	{"/x", "a b", Clobber, "-1:/x=a b"},
	{"/", "a", Missing, "0:/=a"},
	{"/", "a", 123, "123:/=a"},
}

var DelKVRM = []kvcm{
	{"/", "", Clobber, "-1:/"},
	{"/x", "", Clobber, "-1:/x"},
	{"/", "", Missing, "0:/"},
	{"/", "", 123, "123:/"},
}

var GoodPaths = []string{
	"/",
	"/x",
	"/x/y",
	"/x/y-z",
	"/x/y.z",
	"/x/0",
}

var BadPaths = []string{
	"",
	"x",
	"/x=",
	"/x y",
	"/x/",
	"/x//y",
}

var BadInstructions = []string{
	"-1:",
	"-1:x",
	"-1:/x y",
	"-1:=",
	"-1:x=",
	"-1:/x y=",
}

// Anything without a colon is a bad mutation because
// it is missing rev.
var BadMutations = []string{
	"",
	"x",
}

var Splits = [][]string{
	{"/"},
	{"/x", "x"},
	{"/x/y/z", "x", "y", "z"},
}


func sync(s *Store, n int64) {
	if ev, _ := s.Wait(Any, n); ev != nil {
		<-ev
	}
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
		assert.Equalf(t, nil, err, "for path %q", k)
	}
}

func TestEncodeSet(t *testing.T) {
	for _, x := range SetKVRM {
		got, err := EncodeSet(x.k, x.v, x.r)
		assert.Equal(t, nil, err)
		assert.Equal(t, x.m, got)
	}
}

func BenchmarkEncodeSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeSet("/x", "a", Clobber)
	}
}

func TestEncodeDel(t *testing.T) {
	for _, x := range DelKVRM {
		got, err := EncodeDel(x.k, x.r)
		assert.Equal(t, nil, err)
		assert.Equal(t, x.m, got)
	}
}

func BenchmarkEncodeDel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeDel("/x", Clobber)
	}
}

func TestDecodeSet(t *testing.T) {
	for _, x := range SetKVRM {
		k, v, r, keep, err := decode(x.m)
		assert.Equal(t, nil, err)
		assert.Equal(t, true, keep, "keep from "+x.m)
		assert.Equal(t, x.k, k, "key from "+x.m)
		assert.Equal(t, x.v, v, "value from "+x.m)
		assert.Equal(t, x.r, r, "rev from "+x.m)
	}
}

func TestDecodeDel(t *testing.T) {
	for _, x := range DelKVRM {
		k, v, r, keep, err := decode(x.m)
		assert.Equal(t, nil, err)
		assert.Equal(t, false, keep, "keep from "+x.m)
		assert.Equal(t, x.k, k, "key from "+x.m)
		assert.Equal(t, "", v, "value from "+x.m)
		assert.Equal(t, x.r, r, "rev from "+x.m)
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
	st := New()
	defer close(st.Ops)
	v, rev := st.Get("/x")
	assert.Equal(t, Missing, rev)
	assert.Equal(t, []string{""}, v)
}

func TestGet(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	sync(st, 1)
	v, rev := st.Get("/x")
	assert.Equal(t, int64(1), rev)
	assert.Equal(t, []string{"a"}, v)
}

func TestGetDeleted(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeDel("/x", Clobber)}
	sync(st, 2)
	v, rev := st.Get("/x")
	assert.Equal(t, Missing, rev)
	assert.Equal(t, []string{""}, v)
}

func TestSnap(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)
	st.Ops <- Op{1, mut}
	<-st.Seqns // ensure it has been applied before grabbing the snapshot

	_, snap := st.Snap()

	root, ok := snap.(node)
	assert.Equal(t, true, ok)

	exp, _ := emptyDir.apply(1, mut)
	assert.Equal(t, exp, root)
}

func TestApplyInOrder(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/x", "b", Clobber)}
	sync(st, 2)
	v, rev := st.Get("/x")
	assert.Equal(t, int64(2), rev)
	assert.Equal(t, []string{"b"}, v)
}

func BenchmarkApply(b *testing.B) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)

	n := b.N + 1
	for i := 1; i < n; i++ {
		st.Ops <- Op{int64(i), mut}
	}
}

func TestGetSyncOne(t *testing.T) {
	chV := make(chan []string)
	chRev := make(chan int64)
	st := New()
	defer close(st.Ops)
	go func() {
		sync(st, 5)
		v, rev := st.Get("/x")
		chV <- v
		chRev <- rev
	}()
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{5, MustEncodeSet("/x", "b", Clobber)}
	sync(st, 5)
	assert.Equal(t, []string{"b"}, <-chV)
	assert.Equal(t, int64(5), <-chRev)
}

func TestGetSyncSeveral(t *testing.T) {
	chV := make(chan []string)
	chRev := make(chan int64)
	st := New()
	defer close(st.Ops)
	go func() {
		sync(st, 1)
		v, rev := st.Get("/x")
		chV <- v
		chRev <- rev

		sync(st, 5)
		v, rev = st.Get("/x")
		chV <- v
		chRev <- rev

		sync(st, 0)
		v, rev = st.Get("/x")
		chV <- v
		chRev <- rev
	}()

	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{5, MustEncodeSet("/x", "b", Clobber)}

	v := <-chV
	assert.Equal(t, 1, len(v))
	assert.T(t, "a" == v[0] || "b" == v[0])
	n := <-chRev
	assert.T(t, n >= 1)

	assert.Equal(t, []string{"b"}, <-chV)
	assert.Equal(t, int64(5), <-chRev)
	assert.Equal(t, []string{"b"}, <-chV)
	assert.Equal(t, int64(5), <-chRev)
}

func TestGetSyncExtra(t *testing.T) {
	chV := make(chan []string)
	chRev := make(chan int64)
	st := New()
	defer close(st.Ops)

	go func() {
		v, rev := st.Get("/x")
		chV <- v
		chRev <- rev

		sync(st, 5)
		v, rev = st.Get("/x")
		chV <- v
		chRev <- rev

		sync(st, 0)
		v, rev = st.Get("/x")
		chV <- v
		chRev <- rev
	}()

	// Assert here to ensure correct ordering
	assert.Equal(t, []string{""}, <-chV)
	assert.Equal(t, Missing, <-chRev)

	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/x", "a", Clobber)}
	// 5 is below
	st.Ops <- Op{6, MustEncodeSet("/x", "c", Clobber)}
	st.Ops <- Op{7, MustEncodeSet("/x", "c", Clobber)}
	st.Ops <- Op{8, MustEncodeSet("/x", "c", Clobber)}
	// do 5 last
	st.Ops <- Op{5, MustEncodeSet("/x", "b", Clobber)}

	v := <-chV
	assert.Equal(t, 1, len(v))
	assert.T(t, "b" == v[0] || "c" == v[0])
	n := <-chRev
	assert.T(t, n >= 5)

	v = <-chV
	assert.Equal(t, 1, len(v))
	assert.T(t, "b" == v[0] || "c" == v[0])
	n = <-chRev
	assert.T(t, n >= 5)
}

func TestApplyBadThenGood(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, "foo"} // bad mutation
	st.Ops <- Op{2, MustEncodeSet("/x", "b", Clobber)}
	sync(st, 2)
	v, rev := st.Get("/x")
	assert.Equal(t, int64(2), rev)
	assert.Equal(t, []string{"b"}, v)
}

func TestApplyOutOfOrder(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{2, MustEncodeSet("/x", "b", Clobber)}
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}

	sync(st, 2)
	v, rev := st.Get("/x")
	assert.Equal(t, int64(2), rev)
	assert.Equal(t, []string{"b"}, v)
}

func TestApplyIgnoreDuplicate(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{1, MustEncodeSet("/x", "b", Clobber)}
	sync(st, 1)
	v, rev := st.Get("/x")
	assert.Equal(t, int64(1), rev)
	assert.Equal(t, []string{"a"}, v)

	// check that we aren't leaking memory
	assert.Equal(t, 0, st.todo.Len())
}

func TestApplyIgnoreDuplicateOutOfOrder(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/x", "b", Clobber)}
	st.Ops <- Op{1, MustEncodeSet("/x", "c", Clobber)}
	sync(st, 1)
	v, rev := st.Get("/x")
	assert.Equal(t, int64(2), rev)
	assert.Equal(t, []string{"b"}, v)

	// check that we aren't leaking memory
	assert.Equal(t, 0, st.todo.Len())
}

func TestGetWithDir(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/y", "b", Clobber)}
	sync(st, 2)
	dents, rev := st.Get("/")
	assert.Equal(t, Dir, rev)
	sort.SortStrings(dents)
	assert.Equal(t, []string{"x", "y"}, dents)
}

func TestStatWithDir(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/y", "b", Clobber)}
	sync(st, 2)

	ln, rev := st.Stat("/")
	assert.Equal(t, Dir, rev)
	assert.Equal(t, int32(2), ln)
}

func TestStatWithFile(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, MustEncodeSet("/x", "123", Clobber)}
	sync(st, 1)

	ln, rev := st.Stat("/x")
	assert.Equal(t, int64(1), rev)
	assert.Equal(t, int32(3), ln)
}

func TestStatForMissing(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ln, rev := st.Stat("/not/here")
	assert.Equal(t, Missing, rev)
	assert.Equal(t, int32(0), ln)
}

func TestStatWithBadPath(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ln, rev := st.Stat(" #@!$# 213$!")
	assert.Equal(t, Missing, rev)
	assert.Equal(t, int32(0), ln)
}

func TestDirParents(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{1, MustEncodeSet("/x/y/z", "a", Clobber)}
	sync(st, 1)

	dents, rev := st.Get("/")
	assert.Equal(t, Dir, rev)
	assert.Equal(t, []string{"x"}, dents)

	dents, rev = st.Get("/x")
	assert.Equal(t, Dir, rev)
	assert.Equal(t, []string{"y"}, dents)

	dents, rev = st.Get("/x/y")
	assert.Equal(t, Dir, rev)
	assert.Equal(t, []string{"z"}, dents)

	v, rev := st.Get("/x/y/z")
	assert.Equal(t, int64(1), rev)
	assert.Equal(t, []string{"a"}, v)
}

func TestDelDirParents(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{1, MustEncodeSet("/x/y/z", "a", Clobber)}

	st.Ops <- Op{2, MustEncodeDel("/x/y/z", Clobber)}
	sync(st, 2)

	v, rev := st.Get("/")
	assert.Equal(t, Dir, rev)
	assert.Equal(t, []string{""}, v, "lookup /")

	v, rev = st.Get("/x")
	assert.Equal(t, Missing, rev)
	assert.Equal(t, []string{""}, v, "lookup /x")

	v, rev = st.Get("/x/y")
	assert.Equal(t, Missing, rev)
	assert.Equal(t, []string{""}, v, "lookup /x/y")

	v, rev = st.Get("/x/y/z")
	assert.Equal(t, Missing, rev)
	assert.Equal(t, []string{""}, v, "lookup /x/y/z")
}

func TestWaitGlobOnPre(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch, err := st.Wait(MustCompileGlob("/x"), 1)
	if err != nil {
		panic(err)
	}
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{3, mut3}

	exp := clearGetter(<-ch)
	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, exp)
}

func TestWaitGlobAfterPre(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch, err := st.Wait(MustCompileGlob("/x"), 1)
	if err != nil {
		panic(err)
	}
	mut1 := MustEncodeSet("/y", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{3, mut3}

	exp := clearGetter(<-ch)
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, exp)
}

func TestWaitGlobOnPost(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{3, mut3}

	ch, err := st.Wait(MustCompileGlob("/x"), 1)
	if err != nil {
		panic(err)
	}
	exp := clearGetter(<-ch)
	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, exp)
}

func TestWaitGlobAfterPost(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut1 := MustEncodeSet("/y", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{3, mut3}

	ch, err := st.Wait(MustCompileGlob("/x"), 1)
	if err != nil {
		panic(err)
	}
	exp := clearGetter(<-ch)
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, exp)
}


func TestStoreNopEvent(t *testing.T) {
	st := New()
	defer close(st.Ops)

	c, _ := st.Wait(Any, 1)

	st.Ops <- Op{1, Nop}

	ev := <-c
	assert.Equal(t, int64(1), ev.Seqn)
	assert.Equal(t, "/", ev.Path)
	assert.Equal(t, "nop", ev.Desc())
	assert.T(t, ev.IsNop())
}


func TestStoreFlush(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{2, MustEncodeSet("/x", "b", Clobber)}
	st.Flush() // should flush

	assert.Equal(t, int64(2), <-st.Seqns)

	v, rev := st.Get("/x")
	assert.Equal(t, int64(2), rev)
	assert.Equal(t, []string{"b"}, v)
}


func TestStoreNoEventsOnFlush(t *testing.T) {
	st := New()
	defer close(st.Ops)

	ch, err := st.Wait(Any, 1)
	if err != nil {
		panic(err)
	}

	st.Ops <- Op{2, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/x", "b", Clobber)}
	st.Flush()
	assert.Equal(t, int64(3), (<-ch).Seqn)
}


func TestWaitClose(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Wait(Any, 1)
	assert.Equal(t, 1, <-st.Waiting)

	st.Ops <- Op{1, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{2, Nop}
	assert.Equal(t, 0, <-st.Waiting)
}


func TestStoreWaitWorks(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)

	c, _ := st.Wait(Any, 1)
	st.Ops <- Op{1, mut}

	got := <-c
	assert.Equal(t, int64(1), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut, got.Mut)
	assert.Equal(t, 0, st.todo.Len())
	assert.Equal(t, 0, <-st.Waiting)
}

func TestStoreWaitBadInstruction(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := BadInstructions[0]

	statusCh, _ := st.Wait(Any, 1)
	st.Ops <- Op{1, mut}

	got := <-statusCh
	assert.Equal(t, int64(1), got.Seqn)
	assert.Equal(t, ErrBadPath, got.Err)
	assert.Equal(t, mut, got.Mut)
}

func TestStoreWaitRevMatchAdd(t *testing.T) {
	mut := MustEncodeSet("/a", "foo", Missing)

	st := New()
	defer close(st.Ops)

	statusCh, _ := st.Wait(Any, 1)
	st.Ops <- Op{1, mut}

	got := <-statusCh
	assert.Equal(t, int64(1), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut, got.Mut)
}

func TestStoreWaitRevMatchReplace(t *testing.T) {
	mut1 := MustEncodeSet("/a", "foo", Clobber)
	mut2 := MustEncodeSet("/a", "foo", 1)

	st := New()
	defer close(st.Ops)

	statusCh, _ := st.Wait(Any, 2)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}

	got := <-statusCh
	assert.Equal(t, int64(2), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut2, got.Mut)
}

func TestStoreWaitRevMismatchMissing(t *testing.T) {
	mut := MustEncodeSet("/a", "foo", -123)

	st := New()
	defer close(st.Ops)

	statusCh, _ := st.Wait(Any, 1)
	st.Ops <- Op{1, mut}

	got := <-statusCh
	assert.Equal(t, int64(1), got.Seqn)
	assert.Equal(t, ErrRevMismatch, got.Err)
	assert.Equal(t, mut, got.Mut)
}

func TestStoreWaitRevMismatchReplace(t *testing.T) {
	mut1 := MustEncodeSet("/a", "foo", Clobber)
	mut2 := MustEncodeSet("/a", "foo", 0)

	st := New()
	defer close(st.Ops)

	statusCh, _ := st.Wait(Any, 2)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}

	got := <-statusCh
	assert.Equal(t, int64(2), got.Seqn)
	assert.Equal(t, ErrRevMismatch, got.Err)
	assert.Equal(t, mut2, got.Mut)
}

func TestStoreClose(t *testing.T) {
	st := New()
	ch, err := st.Wait(MustCompileGlob("/a/b/c"), 1)
	if err != nil {
		panic(err)
	}
	close(st.Ops)
	assert.Equal(t, Event{}, <-ch)
	assert.T(t, closed(ch))
}

func TestStoreKeepsLog(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)
	st.Ops <- Op{1, mut}
	ch, _ := st.Wait(Any, 1)
	ev := <-ch
	assert.Equal(t, Event{1, "/x", "a", 1, mut, nil, nil}, clearGetter(ev))
}

func TestStoreClean(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)
	st.Ops <- Op{1, mut}

	st.Clean(1)

	ch, err := st.Wait(Any, 1)
	assert.Equal(t, ErrTooLate, err)
	assert.Equal(t, (<-chan Event)(nil), ch)
}

func TestStoreSeqn(t *testing.T) {
	st := New()
	defer close(st.Ops)
	assert.Equal(t, int64(0), <-st.Seqns)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	assert.Equal(t, int64(1), <-st.Seqns)
}
