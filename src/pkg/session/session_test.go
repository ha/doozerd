package session

import (
	"github.com/bmizerany/assert"
	"doozer/store"
	"doozer/test"
	"sort"
	"testing"
)

func TestSession(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	tc := make(chan int64, 1)
	defer close(tc)
	go Clean(st, fp, tc)

	ch := st.Watch(store.MustCompileGlob("/session/*"))

	// check in in the past.
	go fp.Propose([]byte(store.MustEncodeSet("/session/a", "1", store.Clobber)))

	// this is the check-in from above.
	assert.T(t, (<-ch).IsSet())

	tc <- 2 // send a value greater than the value of /session/a.

	ev := <-ch
	assert.T(t, ev.IsDel())
	assert.Equal(t, "/session/a", ev.Path)
}


func TestExpired(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{1, store.MustEncodeSet("/session/a", "1", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/session/b", "2", 0)}
	st.Ops <- store.Op{3, store.MustEncodeSet("/session/c", "3", 0)}
	st.Ops <- store.Op{4, store.MustEncodeSet("/session/d", "4", 0)}
	st.Ops <- store.Op{5, store.MustEncodeSet("/session/e", "5", 0)}
	st.Ops <- store.Op{6, store.MustEncodeSet("/session/f", "6", 0)}
	st.Ops <- store.Op{7, store.MustEncodeSet("/session/g", "7", 0)}
	st.Ops <- store.Op{8, store.MustEncodeSet("/session/h", "8", 0)}
	st.Ops <- store.Op{9, store.MustEncodeSet("/session/i", "9", 0)}
	<-st.Seqns

	got := expired(st, 5)
	exp := map[string]int64{
		"/session/a": 1,
		"/session/b": 2,
		"/session/c": 3,
		"/session/d": 4,
	}
	assert.Equal(t, exp, got)
}


func TestDelAll(t *testing.T) {
	files := map[string]int64{
		"/session/a": 1,
		"/session/b": 2,
		"/session/c": 3,
		"/session/d": 4,
	}

	cp := make(chanProposer, 100)
	delAll(cp, files)
	cp <- "the end"

	got := []string{<-cp, <-cp, <-cp, <-cp}
	exp := []string{
		store.MustEncodeDel("/session/a", 1),
		store.MustEncodeDel("/session/b", 2),
		store.MustEncodeDel("/session/c", 3),
		store.MustEncodeDel("/session/d", 4),
	}
	sort.SortStrings(got)
	sort.SortStrings(exp)
	assert.Equal(t, exp, got)
	assert.Equal(t, "the end", <-cp)
}


type chanProposer chan string


func (cp chanProposer) Propose(v []byte) (e store.Event) {
	cp <- string(v)
	return
}
