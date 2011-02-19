package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestLearnsAValueWithAQuorumOfOne(t *testing.T) {
	var ln learner
	ln.init(1)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}

func TestLearnsOkStickyInSameRound(t *testing.T) {
	var ln learner
	ln.init(1)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)

	m, v, ok = ln.deliver(newVoteFrom("b", 1, "bar"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
}

func TestLearnsOkStickyInNewRound(t *testing.T) {
	var ln learner
	ln.init(1)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)

	m, v, ok = ln.deliver(newVoteFrom("a", 2, "bar"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
}

func TestLearnsAValueWithAQuorumOfTwo(t *testing.T) {
	var ln learner
	ln.init(2)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}

func TestIgnoresMalformedMessageBadRoundNumber(t *testing.T) {
	var ln learner
	ln.init(1)

	m, v, ok := ln.deliver(newVoteFrom("a", 0, "bar"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}

func TestIgnoresMultipleMessagesFromSameSender(t *testing.T) {
	var ln learner
	ln.init(2)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}

func TestIgnoresSenderInOldRound(t *testing.T) {
	var ln learner
	ln.init(2)

	m, v, ok := ln.deliver(newVoteFrom("a", 2, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}

func TestResetsVotedFlags(t *testing.T) {
	var ln learner
	ln.init(2)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("a", 2, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}

func TestResetsVoteCounts(t *testing.T) {
	var ln learner
	ln.init(3)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("a", 2, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 2, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("c", 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}

func TestLearnsATheBestOfTwoValuesInSameRound(t *testing.T) {
	var ln learner
	ln.init(2)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("c", 1, "bar"))
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestBringsOrderOutOfChaos(t *testing.T) {
	var ln learner
	ln.init(2)

	m, v, ok := ln.deliver(newVoteFrom("a", 1, "bar")) //valid
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
	m, v, ok = ln.deliver(newVoteFrom("c", 2, "funk")) //reset
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
	m, v, ok = ln.deliver(newVoteFrom("b", 1, "bar")) //ignored
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(newVoteFrom("c", 1, "foo")) //ignored
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
	m, v, ok = ln.deliver(newVoteFrom("b", 2, "foo")) //valid
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
	m, v, ok = ln.deliver(newVoteFrom("a", 2, "foo")) //valid (at quorum)
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, &M{Cmd: learn, Value: []byte("foo")}, m)
}


func TestLearnerIgnoresBadMessages(t *testing.T) {
	var ln learner

	m, v, ok := ln.deliver(packet{})
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(packet{M: M{Cmd: vote}}) // missing Vrnd
	assert.Equal(t, false, ln.done)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
}


func TestSinkLearnsAValue(t *testing.T) {
	var ln learner

	m, v, ok := ln.deliver(packet{M: *newLearn("foo")})
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, (*M)(nil), m)
}

func TestSinkLearnsOkSticky(t *testing.T) {
	var ln learner

	m, v, ok := ln.deliver(packet{M: *newLearn("foo")})
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte("foo"), v)
	assert.Equal(t, true, ok)
	assert.Equal(t, (*M)(nil), m)

	m, v, ok = ln.deliver(packet{M: *newLearn("bar")})
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
	assert.Equal(t, []byte{}, v)
	assert.Equal(t, false, ok)
	assert.Equal(t, (*M)(nil), m)
}
