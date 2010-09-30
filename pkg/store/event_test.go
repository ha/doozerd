package store

import (
	"junta/assert"
	"testing"
)

func TestEventIsSet(t *testing.T) {
	p, v := "/x", "a"
	m := MustEncodeSet(p, v, Clobber)
	ev := Event{1, p, v, "1", m, nil, nil}
	assert.Equal(t, true, ev.IsSet())
	assert.Equal(t, false, ev.IsDel())
	assert.Equal(t, false, ev.IsDummy())
}

func TestEventIsDel(t *testing.T) {
	p := "/x"
	m := MustEncodeDel(p, Clobber)
	ev := Event{1, p, "", Missing, m, nil, nil}
	assert.Equal(t, true, ev.IsDel())
	assert.Equal(t, false, ev.IsSet())
	assert.Equal(t, false, ev.IsDummy())
}

func TestEventIsDummy(t *testing.T) {
	ev := Event{Seqn:1, Err:ErrTooLate}
	assert.Equal(t, true, ev.IsDummy())
	assert.Equal(t, false, ev.IsSet())
	assert.Equal(t, false, ev.IsDel())
}
