package timer

import (
	"github.com/bmizerany/assert"
	"doozer/store"
	"testing"
	"time"
	"strconv"
)

var testGlob = store.MustCompileGlob("/timer/**")

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
	timer := New(testGlob, OneMillisecond, st)
	defer timer.Stop()

	st.Ops <- store.Op{1, encodeTimer("/timer/longest", 40*OneMillisecond)}
	st.Ops <- store.Op{2, encodeTimer("/timer/short", 10*OneMillisecond)}
	st.Ops <- store.Op{3, encodeTimer("/timer/long", 25*OneMillisecond)}

	got := <-timer.C
	assert.Equal(t, got.Path, "/timer/short")
	assert.Equal(t, int64(2), got.Cas)
	assert.T(t, got.At <= time.Nanoseconds())

	got = <-timer.C
	assert.Equal(t, got.Path, "/timer/long")
	assert.Equal(t, int64(3), got.Cas)
	assert.T(t, got.At <= time.Nanoseconds())

	got = <-timer.C
	assert.Equal(t, got.Path, "/timer/longest")
	assert.Equal(t, int64(1), got.Cas)
	assert.T(t, got.At <= time.Nanoseconds())

	assert.Equal(t, 0, timer.ticks.Len())
}

func TestDeleteTimer(t *testing.T) {
	st := store.New()
	timer := New(testGlob, OneMillisecond, st)
	defer timer.Stop()

	never := "/timer/never/ticks"
	does := "/timer/does/tick"

	// Wait one minute to ensure it doesn't tick before
	// the following delete and assert.
	st.Ops <- store.Op{1, encodeTimer(never, 30*OneMillisecond)}

	st.Ops <- store.Op{2, encodeTimer(does, 60*OneMillisecond)}

	st.Ops <- store.Op{3, store.MustEncodeDel(never, store.Clobber)}

	// If the first timer failed to delete, it would come out first.
	got := (<-timer.C)
	assert.Equal(t, does, got.Path)
	assert.Equal(t, int64(2), got.Cas)
}

func TestUpdate(t *testing.T) {
	st := store.New()
	timer := New(testGlob, OneMillisecond, st)
	defer timer.Stop()

	st.Ops <- store.Op{1, encodeTimer("/timer/y", 90*OneMillisecond)}
	st.Ops <- store.Op{2, encodeTimer("/timer/x", 30*OneMillisecond)}
	st.Ops <- store.Op{3, encodeTimer("/timer/x", 60*OneMillisecond)}

	// The deadline scheduled from seqn 2 should never fire. It should be
	// replaced by seqn 3.

	got := (<-timer.C)
	assert.Equal(t, "/timer/x", got.Path)
	assert.Equal(t, int64(3), got.Cas)
	got = (<-timer.C)
	assert.Equal(t, "/timer/y", got.Path)
	assert.Equal(t, int64(1), got.Cas)
}

func TestTimerStop(t *testing.T) {
	st := store.New()
	timer := New(testGlob, OneMillisecond, st)
	timer.Stop()

	st.Ops <- store.Op{1, encodeTimer("/timer/y", 90*OneMillisecond)}
	st.Ops <- store.Op{2, encodeTimer("/timer/x", 30*OneMillisecond)}
	st.Wait(1)

	assert.Equal(t, 0, <-st.Watches) // From seqn 1
}
