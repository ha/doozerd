package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestSinkLearnsAValue(t *testing.T) {
	var s sink

	s.Put(newLearn("foo"))
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)
}

func TestSinkLearnsOkSticky(t *testing.T) {
	var s sink

	s.Put(newLearn("foo"))
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)

	s.Put(newLearn("bar"))
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)
}

func TestSinkIgnoresOtherMessages(t *testing.T) {
	var s sink

	s.Put(newVote(1, "foo"))
	assert.Equal(t, false, s.done)

	s.Put(newLearn("foo"))
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)
}


func TestSinkIgnoresBadMessages(t *testing.T) {
	var s sink

	s.Put(&M{})
	assert.Equal(t, false, s.done)
}
