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
	if ev, _ := s.Wait(n); ev != nil {
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
		_, ok := err.(*BadPathError)
		assert.Tf(t, ok, "for path %q, got %T: %v", k, err, err)
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
		_, ok := err.(*BadPathError)
		assert.Tf(t, ok, "for mut %q, got %T: %v", m, err, err)
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
		sync(st, 0)
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

func TestWatchSetSimple(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(MustCompileGlob("/x"))
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{3, mut3}

	expa := clearGetter(<-ch)
	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, expa)
	expb := clearGetter(<-ch)
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, expb)
}

func TestWatchSetOutOfOrder(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(MustCompileGlob("/x"))
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{3, mut3}

	expa := clearGetter(<-ch)
	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, expa)
	expb := clearGetter(<-ch)
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, expb)
}

func TestWatchDel(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(MustCompileGlob("/x"))
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	mut4 := MustEncodeDel("/x", Clobber)
	mut5 := MustEncodeDel("/y", Clobber)
	mut6 := MustEncodeDel("/x", Clobber)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{3, mut3}
	st.Ops <- Op{4, mut4}
	st.Ops <- Op{5, mut5}
	st.Ops <- Op{6, mut6}

	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil, nil}, clearGetter(<-ch))
}

func TestWatchAddSimple(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(MustCompileGlob("/*"))
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{3, mut3}

	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", 3, mut3, nil, nil}, clearGetter(<-ch))
}

func TestWatchAddOutOfOrder(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(MustCompileGlob("/*"))
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	st.Ops <- Op{3, mut3}
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}

	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", 3, mut3, nil, nil}, clearGetter(<-ch))
}

func TestWatchRem(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(MustCompileGlob("/*"))
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	mut4 := MustEncodeDel("/x", Clobber)
	mut5 := MustEncodeDel("/y", Clobber)
	mut6 := MustEncodeDel("/x", Clobber)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{3, mut3}
	st.Ops <- Op{4, mut4}
	st.Ops <- Op{5, mut5}
	st.Ops <- Op{6, mut6}

	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", 3, mut3, nil, nil}, clearGetter(<-ch))

	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{5, "/y", "", Missing, mut5, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil, nil}, clearGetter(<-ch))
}

func TestWatchSetDirParents(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(MustCompileGlob("/x/**"))
	mut1 := MustEncodeSet("/x/y/z", "a", Clobber)
	st.Ops <- Op{1, mut1}

	assert.Equal(t, Event{1, "/x/y/z", "a", 1, mut1, nil, nil}, clearGetter(<-ch))
}

func TestWatchDelDirParents(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(Any)
	mut1 := MustEncodeSet("/x/y/z", "a", Clobber)
	st.Ops <- Op{1, mut1}

	mut2 := MustEncodeDel("/x/y/z", Clobber)
	st.Ops <- Op{2, mut2}

	assert.Equal(t, Event{1, "/x/y/z", "a", 1, mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x/y/z", "", Missing, mut2, nil, nil}, clearGetter(<-ch))
}

func TestWatchApply(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(Any)
	mut1 := MustEncodeSet("/x", "a", Clobber)
	mut2 := MustEncodeSet("/x", "b", Clobber)
	mut3 := MustEncodeSet("/y", "c", Clobber)
	mut4 := MustEncodeDel("/x", Clobber)
	mut5 := MustEncodeDel("/y", Clobber)
	mut6 := MustEncodeDel("/x", Clobber)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}
	st.Ops <- Op{3, mut3}
	st.Ops <- Op{4, mut4}
	st.Ops <- Op{5, mut5}
	st.Ops <- Op{6, mut6}

	assert.Equal(t, Event{1, "/x", "a", 1, mut1, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{2, "/x", "b", 2, mut2, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{3, "/y", "c", 3, mut3, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{4, "/x", "", Missing, mut4, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{5, "/y", "", Missing, mut5, nil, nil}, clearGetter(<-ch))
	assert.Equal(t, Event{6, "/x", "", Missing, mut6, nil, nil}, clearGetter(<-ch))
}

func TestStoreWaitZero(t *testing.T) {
	st := New()
	defer close(st.Ops)

	ch, err := st.Wait(0)
	assert.Equal(t, ErrTooLate, err)
	assert.Equal(t, (<-chan Event)(nil), ch)
}

func TestStoreNopEvent(t *testing.T) {
	st := New()
	defer close(st.Ops)

	c := make(chan Event, 100)
	w, _ := st.watchOn(Any, c, 1, 100)

	st.Ops <- Op{1, Nop}

	ev := <-w.C
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

	ch := st.Watch(Any)
	assert.Equal(t, 1, <-st.Watches)

	st.Ops <- Op{2, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/x", "b", Clobber)}
	st.Flush()
	assert.Equal(t, int64(3), (<-ch).Seqn)
}


func TestWaitClose(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Wait(1)
	assert.Equal(t, 1, <-st.Watches)

	st.Ops <- Op{1, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{2, Nop}
	assert.Equal(t, 0, <-st.Watches)
}

func TestSyncPathClose(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := make(chan int)

	go func() {
		st.SyncPath("/x")
		ch <- 1
	}()

	for {
		st.Ops <- Op{0, ""} // just for synchronization
		x := st.watches
		if len(x) > 0 {
			break
		}
	}

	st.Ops <- Op{1, MustEncodeSet("/x", "", Clobber)}

	<-ch

	st.Ops <- Op{2, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{0, ""} // just for synchronization

	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWaitWorks(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)

	c, _ := st.Wait(1)
	st.Ops <- Op{1, mut}

	got := <-c
	assert.Equal(t, int64(1), got.Seqn)
	assert.Equal(t, nil, got.Err)
	assert.Equal(t, mut, got.Mut)
	assert.Equal(t, 0, st.todo.Len())
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWaitDoesntBlock(t *testing.T) {
	st := New()
	defer close(st.Ops)

	_, _ = st.Wait(3) // never read from this chan

	w := NewWatch(st, Any) // be sure we can get all values from w

	for i := int64(1); i < 6; i++ {
		st.Ops <- Op{i, MustEncodeSet("/x", "a", Clobber)}
	}

	for i := int64(1); i < 6; i++ {
		ev := <-w.C
		assert.Equal(t, i, ev.Seqn)
	}
}

func TestStoreWaitOutOfOrder(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := st.Watch(Any)
	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/x", "b", Clobber)}

	assert.Equal(t, int64(1), (<-ch).Seqn)
	assert.Equal(t, int64(2), (<-ch).Seqn)
}

func TestStoreWaitBadMutation(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := BadMutations[0]

	c, _ := st.Wait(1)
	st.Ops <- Op{1, mut}

	got := <-c
	assert.Equal(t, int64(1), got.Seqn)
	assert.Equal(t, ErrBadMutation, got.Err)
	assert.Equal(t, mut, got.Mut)
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWaitBadInstruction(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := BadInstructions[0]

	statusCh, _ := st.Wait(1)
	st.Ops <- Op{1, mut}

	got := <-statusCh
	assert.Equal(t, int64(1), got.Seqn)
	_, ok := got.Err.(*BadPathError)
	assert.Tf(t, ok, "for mut %q, got %T: %v", mut, got.Err, got.Err)
	assert.Equal(t, mut, got.Mut)
}

func TestStoreWaitRevMatchAdd(t *testing.T) {
	mut := MustEncodeSet("/a", "foo", Missing)

	st := New()
	defer close(st.Ops)

	statusCh, _ := st.Wait(1)
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

	statusCh, _ := st.Wait(2)
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

	statusCh, _ := st.Wait(1)
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

	statusCh, _ := st.Wait(2)
	st.Ops <- Op{1, mut1}
	st.Ops <- Op{2, mut2}

	got := <-statusCh
	assert.Equal(t, int64(2), got.Seqn)
	assert.Equal(t, ErrRevMismatch, got.Err)
	assert.Equal(t, mut2, got.Mut)
}

func TestSyncPathFuture(t *testing.T) {
	st := New()
	done := make(chan bool, 1)

	go func() {
		for <-st.Watches < 1 {
		} // make sure SyncPath gets in there first
		st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
		st.Ops <- Op{2, MustEncodeSet("/y", "b", Clobber)}
		st.Ops <- Op{3, MustEncodeSet("/y", "c", Clobber)}
		st.Ops <- Op{4, MustEncodeSet("/z", "d", Clobber)}
		<-done
		close(st.Ops)
	}()

	g, err := st.SyncPath("/y")
	assert.Equal(t, nil, err)
	got := GetString(g, "/y")
	assert.Equal(t, "b", got)
	done <- true
}

func TestSyncPathImmediate(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{1, MustEncodeSet("/x", "a", Clobber)}
	st.Ops <- Op{2, MustEncodeSet("/y", "b", Clobber)}

	g, err := st.SyncPath("/y")
	assert.Equal(t, nil, err)
	got := GetString(g, "/y")
	assert.Equal(t, "b", got)
}

func TestStoreClose(t *testing.T) {
	st := New()
	ch := st.Watch(MustCompileGlob("/a/b/c"))
	close(st.Ops)
	assert.Equal(t, Event{}, <-ch)
	assert.T(t, closed(ch))
}

func TestStoreKeepsLog(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)
	st.Ops <- Op{1, mut}
	ch, _ := st.Wait(1)
	ev := <-ch
	assert.Equal(t, Event{1, "/x", "a", 1, mut, nil, nil}, clearGetter(ev))
}

func TestStoreClean(t *testing.T) {
	st := New()
	defer close(st.Ops)
	mut := MustEncodeSet("/x", "a", Clobber)
	st.Ops <- Op{1, mut}

	st.Clean(1)

	ch, err := st.Wait(1)
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

func TestStoreNoDeadlock(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Watch(Any)
	st.Ops <- Op{1, Nop}
	<-st.Seqns
}

func TestStoreWatchIntervalLog(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := make(chan Event)

	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{5, Nop}

	st.watchOn(Any, ch, 3, 5)
	assert.Equal(t, 0, <-st.Watches)
	ev := <-ch
	assert.Equal(t, int64(3), ev.Seqn)
	assert.Equal(t, "/x", ev.Path)
	ev = <-ch
	assert.Equal(t, int64(4), ev.Seqn)
	assert.Equal(t, "/x", ev.Path)
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWatchIntervalFuture(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := make(chan Event)

	go func() {
		for <-st.Watches < 1 {
		}
		st.Ops <- Op{1, Nop}
		st.Ops <- Op{2, Nop}
		st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
		st.Ops <- Op{4, MustEncodeSet("/x", "", Clobber)}
		st.Ops <- Op{5, Nop}
	}()

	st.watchOn(Any, ch, 3, 5)
	ev := <-ch
	assert.Equal(t, int64(3), ev.Seqn)
	assert.Equal(t, "/x", ev.Path)
	ev = <-ch
	assert.Equal(t, int64(4), ev.Seqn)
	assert.Equal(t, "/x", ev.Path)
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWatchIntervalTrans(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := make(chan Event)

	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
	go func() {
		for <-st.Watches < 1 {
		}
		st.Ops <- Op{4, MustEncodeSet("/x", "", Clobber)}
		st.Ops <- Op{5, Nop}
	}()

	st.watchOn(Any, ch, 3, 5)
	ev := <-ch
	assert.Equal(t, int64(3), ev.Seqn)
	assert.Equal(t, "/x", ev.Path)
	ev = <-ch
	assert.Equal(t, int64(4), ev.Seqn)
	assert.Equal(t, "/x", ev.Path)
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWatchIntervalTooLate(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	st.Ops <- Op{3, Nop}
	st.Ops <- Op{4, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{5, Nop}
	st.Clean(3)

	w, err := st.watchOn(Any, make(chan Event), 2, 5)
	assert.Equal(t, ErrTooLate, err)
	assert.Equal(t, (*Watch)(nil), w)
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWatchIntervalWaitFuture(t *testing.T) {
	st := New()
	defer close(st.Ops)
	ch := make(chan Event, 1)
	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}

	st.watchOn(Any, ch, 3, 4)
	ev := <-ch
	assert.Equal(t, int64(3), ev.Seqn)
	assert.Equal(t, "/x", ev.Path)
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWatchIntervalWaitTooLate(t *testing.T) {
	st := New()
	defer close(st.Ops)
	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{4, Nop}
	st.Clean(4)

	w, err := st.watchOn(Any, make(chan Event, 1), 3, 4)
	assert.Equal(t, ErrTooLate, err)
	assert.Equal(t, (*Watch)(nil), w)
	assert.Equal(t, 0, <-st.Watches)
}

func TestStoreWatchFrom(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	<-st.Seqns

	ch := st.Watch(Any)
	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
	assert.Equal(t, int64(3), (<-ch).Seqn)
}

func TestStoreStopWatch(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	<-st.Seqns

	wt := NewWatch(st, Any)
	ch := make(chan Event, 2)
	wt.C, wt.c = ch, ch
	assert.Equal(t, 1, <-st.Watches)

	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
	assert.Equal(t, "/x", (<-wt.C).Path)
	wt.Stop()

	st.Ops <- Op{4, MustEncodeSet("/y", "", Clobber)}
	st.Ops <- Op{5, MustEncodeSet("/y", "", Clobber)}

	st.Wait(5)
	assert.Equal(t, 0, <-st.Watches)
	assert.Equal(t, 0, len(wt.C))
}

func TestStoreStopDrainWatch(t *testing.T) {
	st := New()
	defer close(st.Ops)

	st.Ops <- Op{1, Nop}
	st.Ops <- Op{2, Nop}
	<-st.Seqns

	w1 := NewWatch(st, Any)

	st.Ops <- Op{3, MustEncodeSet("/x", "", Clobber)}
	st.Ops <- Op{4, MustEncodeSet("/y", "", Clobber)}
	assert.Equal(t, "/x", (<-w1.C).Path)
	assert.Equal(t, "/y", (<-w1.C).Path)

	w2 := NewWatch(st, Any)

	st.Ops <- Op{5, MustEncodeSet("/a", "", Clobber)}
	st.Ops <- Op{6, MustEncodeSet("/b", "", Clobber)}
	st.Ops <- Op{7, MustEncodeSet("/c", "", Clobber)}
	<-st.Seqns
	w1.Stop()

	st.Ops <- Op{8, MustEncodeSet("/p", "", Clobber)}
	st.Ops <- Op{9, MustEncodeSet("/q", "", Clobber)}
	assert.Equal(t, "/a", (<-w2.C).Path)
	assert.Equal(t, "/b", (<-w2.C).Path)
	assert.Equal(t, "/c", (<-w2.C).Path)
	assert.Equal(t, "/p", (<-w2.C).Path)
	assert.Equal(t, "/q", (<-w2.C).Path)
}

func TestWatchIsStopped(t *testing.T) {
	w := Watch{
		shutdown: make(chan bool, 1),
		stopped:  false,
	}

	// it should begin unstopped
	assert.Equal(t, false, w.isStopped())

	// it should work the first time
	w.shutdown <- true
	assert.Equal(t, true, w.isStopped())

	// it should remember that w has been stopped
	assert.Equal(t, true, w.isStopped())
}
