package member

import (
	"github.com/bmizerany/assert"
	"doozer/store"
	"doozer/test"
	"testing"
)

func TestMemberSimple(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	go Clean(fp.Store, fp)

	// start our session
	fp.Propose(store.MustEncodeSet("/session/a", "foo", store.Missing))

	keys := map[string]string{
		"/doozer/slot/0":    "a",
		"/doozer/members/a": "addr",
		"/doozer/info/a/x":  "a",
		"/doozer/info/a/y":  "b",
	}

	// join the cluster
	for k, p := range keys {
		fp.Propose(store.MustEncodeSet(k, p, store.Missing))
	}

	// watch the keys to be deleted
	ch := fp.Watch("/doozer/**")

	// end the session
	fp.Propose(store.MustEncodeDel("/session/a", store.Clobber))

	// now that the session has ended, check its membership is cleaned up
	for i := 0; i < len(keys); i++ {
		ev := <-ch
		_, ok := keys[ev.Path]
		keys[ev.Path] = "", false
		assert.T(t, ok)
		assert.Equal(t, "", ev.Body)
	}
}
