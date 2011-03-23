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
	c := make(chan string)
	go Clean(c, fp.Store, fp)

	// start our session
	fp.Propose([]byte(store.MustEncodeSet("/ctl/sess/a", "foo", store.Missing)))

	fp.Propose([]byte(store.MustEncodeSet("/ctl/node/a/x", "a", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/ctl/node/a/y", "b", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/ctl/node/a/addr", "addr", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/ctl/cal/0", "a", store.Missing)))

	calCh := fp.Watch(store.MustCompileGlob("/ctl/cal/0"))
	nodeCh := fp.Watch(store.MustCompileGlob("/ctl/node/a/?"))

	// end the session
	go func() { c <- "addr" }()

	ev := <-calCh
	assert.T(t, ev.IsSet())
	assert.Equal(t, "", ev.Body)

	cs := []int{}

	ev = <-nodeCh
	assert.T(t, ev.IsDel())
	cs = append(cs, int(ev.Path[len(ev.Path)-1]))

	ev = <-nodeCh
	assert.T(t, ev.IsDel())
	cs = append(cs, int(ev.Path[len(ev.Path)-1]))

	sort.SortInts(cs)
	assert.Equal(t, []int{'x', 'y'}, cs)
}
