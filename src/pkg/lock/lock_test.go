package lock

import (
	"github.com/bmizerany/assert"
	"doozer/store"
	"doozer/test"
	"testing"
)

func TestLockSimple(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	go Clean(fp, st.Watch(SessGlob))

	// start our session
	fp.Propose([]byte(store.MustEncodeSet("/ctl/sess/a", "1.2.3.4:55", store.Clobber)))

	// lock something for a
	fp.Propose([]byte(store.MustEncodeSet("/lock/x", "a", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/lock/y", "b", store.Missing)))
	fp.Propose([]byte(store.MustEncodeSet("/lock/z", "a", store.Missing)))

	// watch the locks to be deleted
	ch := fp.Watch(store.MustCompileGlob("/lock/*"))

	// end the session
	fp.Propose([]byte(store.MustEncodeDel("/ctl/sess/a", store.Clobber)))

	// now that the session has ended, check all locks it owned are released
	assert.Equal(t, "/lock/x", (<-ch).Path)
	assert.Equal(t, "/lock/z", (<-ch).Path)
}
