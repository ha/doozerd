package timer

import (
	"junta/assert"
	"junta/store"
	"testing"
	"time"
	"strconv"
)

const (
	testPattern = "/timer/**"
)

func encodeTimer(path string, offset int64) string {
	future := time.Nanoseconds() + offset
	muta := store.MustEncodeSet(
		path,
		strconv.Itoa64(future),
		store.Clobber,
	)
	return muta
}

func TestManyOneshotTimers(t *testing.T) {
	st := store.New()
	timer := New(testPattern, OneMillisecond*10, st)
	defer timer.Close()

	st.Apply(1, encodeTimer("/timer/longest", 40*OneMicrosecond))
	st.Apply(2, encodeTimer("/timer/short", 10*OneMicrosecond))
	st.Apply(3, encodeTimer("/timer/long", 25*OneMicrosecond))

	got := <-timer.C
	assert.Equal(t, got.Path, "/timer/short")
	assert.T(t, got.At <= time.Nanoseconds())

	got = <-timer.C
	assert.Equal(t, got.Path, "/timer/long")
	assert.T(t, got.At <= time.Nanoseconds())

	got = <-timer.C
	assert.Equal(t, got.Path, "/timer/longest")
	assert.T(t, got.At <= time.Nanoseconds())

	assert.Equal(t, 0, timer.Len())
}

func TestDeleteTimer(t *testing.T) {
	st := store.New()
	timer := New(testPattern, OneMillisecond*10, st)
	defer timer.Close()

	never := "/timer/never/ticks"

	watch := make(chan store.Event)
	st.Watch(testPattern, watch)

	// Wait one minute to ensure it doesn't tick before
	// the following delete and assert.
	st.Apply(1, encodeTimer(never, 60*OneSecond))
	<-watch

	st.Apply(2, store.MustEncodeDel(never, store.Clobber))
	<-watch

	// Potential race-condition:  The Timer may not have yet
	// deleted the timer.  Thoughts on how to test?
	assert.Equal(t, 0, timer.Len())
}
