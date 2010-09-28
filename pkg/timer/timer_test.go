package timer

import (
	"junta/assert"
	"junta/store"
	"testing"
	"time"
	"strconv"
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

func TestTwoManyOneshotTimers(t *testing.T) {
	st := store.New()
	timer := New("test", oneMillisecond*10, st)
	defer timer.Close()

	st.Apply(1, encodeTimer("/timer/longest", 40*oneMicrosecond))
	st.Apply(2, encodeTimer("/timer/short", 10*oneMicrosecond))
	st.Apply(3, encodeTimer("/timer/long", 25*oneMicrosecond))

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
