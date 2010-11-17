package paxos

import (
	"doozer/assert"
	"testing"
)

func TestSinkLearnsAValue(t *testing.T) {
	var s sink

	s.Put(newLearn("foo"))
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
