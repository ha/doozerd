package member

import (
	"junta/assert"
	"junta/store"
	"junta/test"
	"testing"
)

func TestMemberSimple(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	go Clean(fp.Store, fp)

	// start our session
	fp.Propose(store.MustEncodeSet("/session/a", "foo", store.Missing))

	keys := [][2]string{
		{"/junta/slot/0", "a"},
		{"/junta/members/a", "addr"},
		{"/junta/info/a/x", "a"},
		{"/junta/info/a/y", "b"},
	}

	// join the cluster
	for _, k := range keys {
		fp.Propose(store.MustEncodeSet(k[0], k[1], store.Missing))
	}

	// watch the keys to be deleted
	ch := make(chan store.Event)
	fp.Watch("/junta/**", ch)

	// end the session
	fp.Propose(store.MustEncodeDel("/session/a", store.Clobber))

	// now that the session has ended, check its membership is cleaned up
	for _, k := range keys {
		ev := <-ch
		assert.Equal(t, k[0], ev.Path)
		assert.Equal(t, "", ev.Body, ev.Path)
	}
}
