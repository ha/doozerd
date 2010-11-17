package session

import (
	"doozer/assert"
	"doozer/store"
	"doozer/test"
	"strconv"
	"time"
	"testing"
)

func TestSession(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	go Clean(st, fp)

	ch := make(chan store.Event)
	st.Watch("/session/*", ch)

	// check-in with less than a nanosecond to live
	body := strconv.Itoa64(time.Nanoseconds() + 1)
	fp.Propose(store.MustEncodeSet("/session/a", body, store.Clobber))

	// Throw away the set
	assert.T(t, (<-ch).IsSet())

	ev := <-ch
	assert.T(t, ev.IsDel())
	assert.Equal(t, "/session/a", ev.Path)
}
