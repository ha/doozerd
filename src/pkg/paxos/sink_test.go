package paxos

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestSinkLearnsAValue(t *testing.T) {
	var s sink

	b := s.Put(newLearn("foo"))
	assert.Equal(t, true, b)
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)
}

func TestSinkLearnsOkSticky(t *testing.T) {
	var s sink

	b := s.Put(newLearn("foo"))
	assert.Equal(t, true, b)
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)

	b = s.Put(newLearn("bar"))
	assert.Equal(t, false, b)
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)
}

func TestSinkIgnoresOtherMessages(t *testing.T) {
	var s sink

	b := s.Put(newVote(1, "foo"))
	assert.Equal(t, false, b)
	assert.Equal(t, false, s.done)

	b = s.Put(newLearn("foo"))
	assert.Equal(t, true, b)
	assert.Equal(t, true, s.done)
	assert.Equal(t, "foo", s.v)
}
