package timer

import (
	"junta/assert"
	"junta/store"
	"testing"
	"time"
	"strconv"
)

func TestOneshotTimer(t *testing.T) {
	// Start the timer process
	st := store.New()
	timer := New("test", oneMillisecond*10, st)
	defer timer.Close()

	path := "/timer/foo/bar"
	future := time.Nanoseconds()+(oneMillisecond*50)
	muta := store.MustEncodeSet(
		path,
		strconv.Itoa64(future),
		store.Clobber,
	)

	st.Apply(1, muta)

	<-timer.C
	assert.T(t, future <= time.Nanoseconds())
}

func TestTwoOneshotTimers(t *testing.T) {
	// Start the timer process
	st := store.New()
	timer := New("test", oneMillisecond*10, st)
	defer timer.Close()

	pathA := "/timer/foo/baz"
	futureA := time.Nanoseconds()+(oneSecond)
	mutaA := store.MustEncodeSet(
		pathA,
		strconv.Itoa64(futureA),
		store.Clobber,
	)

	pathB := "/timer/foo/bar"
	futureB := time.Nanoseconds()+(2*oneSecond)
	mutaB := store.MustEncodeSet(
		pathB,
		strconv.Itoa64(futureB),
		store.Clobber,
	)

	st.Apply(1, mutaA)
	st.Apply(2, mutaB)

	got := <-timer.C
	assert.T(t, futureA <= time.Nanoseconds())
	assert.Equal(t, pathA, got.Path)
	assert.Equal(t, futureA, got.At)

	got = <-timer.C
	assert.T(t, futureB <= time.Nanoseconds())
	assert.Equal(t, pathB, got.Path)
	assert.Equal(t, futureB, got.At)
}
