package member

import (
	"github.com/bmizerany/assert"
	"doozer/store"
	"doozer/test"
	"sort"
	"testing"
)

func TestMemberSimple(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	go Clean(fp.Store, fp)

	for <-st.Watches < 1 {
	}

	// start our session
	fp.Propose(store.MustEncodeSet("/session/a", "foo", store.Missing))

	fp.Propose(store.MustEncodeSet("/doozer/info/a/x", "a", store.Missing))
	fp.Propose(store.MustEncodeSet("/doozer/info/a/y", "b", store.Missing))
	fp.Propose(store.MustEncodeSet("/doozer/members/a", "addr", store.Missing))
	fp.Propose(store.MustEncodeSet("/doozer/slot/0", "a", store.Missing))

	slotCh := fp.Watch(store.MustCompileGlob("/doozer/slot/0"))
	membCh := fp.Watch(store.MustCompileGlob("/doozer/members/a"))
	infoCh := fp.Watch(store.MustCompileGlob("/doozer/info/a/?"))

	// end the session
	fp.Propose(store.MustEncodeDel("/session/a", store.Clobber))

	ev := <-slotCh
	assert.T(t, ev.IsSet())
	assert.Equal(t, "", ev.Body)

	ev = <-membCh
	assert.T(t, ev.IsDel())

	cs := []int{}

	ev = <-infoCh
	assert.T(t, ev.IsDel())
	cs = append(cs, int(ev.Path[len(ev.Path)-1]))

	ev = <-infoCh
	assert.T(t, ev.IsDel())
	cs = append(cs, int(ev.Path[len(ev.Path)-1]))

	sort.SortInts(cs)
	assert.Equal(t, []int{'x', 'y'}, cs)
}
