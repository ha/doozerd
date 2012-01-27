package member

import (
	"github.com/bmizerany/assert"
	"github.com/ha/doozerd/store"
	"github.com/ha/doozerd/test"
	"sort"
	"testing"
)

func TestMemberSimple(t *testing.T) {
	st := store.New("")
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	c := make(chan string)
	go Clean(c, fp.Store, fp)

	fp.Propose([]byte(store.MustEncodeSet("/ctl/node/a/x", "a", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/ctl/node/a/y", "b", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/ctl/node/a/addr", "1.2.3.4", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/ctl/cal/0", "a", store.Missing)))

	calCh, err := fp.Wait(store.MustCompileGlob("/ctl/cal/0"), 1+<-fp.Seqns)
	if err != nil {
		panic(err)
	}
	nodeCh, err := fp.Wait(store.MustCompileGlob("/ctl/node/a/?"), 1+<-fp.Seqns)
	if err != nil {
		panic(err)
	}

	// indicate that this peer is inactive
	go func() { c <- "1.2.3.4" }()

	ev := <-calCh
	assert.T(t, ev.IsSet())
	assert.Equal(t, "", ev.Body)

	cs := []int{}

	ev = <-nodeCh
	assert.T(t, ev.IsDel())
	cs = append(cs, int(ev.Path[len(ev.Path)-1]))
	nodeCh, err = fp.Wait(store.MustCompileGlob("/ctl/node/a/?"), ev.Seqn+1)
	if err != nil {
		panic(err)
	}

	ev = <-nodeCh
	assert.T(t, ev.IsDel())
	cs = append(cs, int(ev.Path[len(ev.Path)-1]))

	sort.Ints(cs)
	assert.Equal(t, []int{'x', 'y'}, cs)
}
