package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestLearnsAValueWithAQuorumOfOne(t *testing.T) {
	var ln learner
	ln.init(1)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestLearnsOkStickyInSameRound(t *testing.T) {
	var ln learner
	ln.init(1)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)

	ln.Deliver(newVoteFrom("b", 1, "bar"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestLearnsOkStickyInNewRound(t *testing.T) {
	var ln learner
	ln.init(1)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)

	ln.Deliver(newVoteFrom("a", 2, "bar"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestLearnsAValueWithAQuorumOfTwo(t *testing.T) {
	var ln learner
	ln.init(2)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestIgnoresMalformedMessageBadRoundNumber(t *testing.T) {
	var ln learner
	ln.init(1)

	ln.Deliver(newVoteFrom("a", 0, "bar"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestIgnoresMultipleMessagesFromSameSender(t *testing.T) {
	var ln learner
	ln.init(2)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestIgnoresSenderInOldRound(t *testing.T) {
	var ln learner
	ln.init(2)

	ln.Deliver(newVoteFrom("a", 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestResetsVotedFlags(t *testing.T) {
	var ln learner
	ln.init(2)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("a", 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestResetsVoteCounts(t *testing.T) {
	var ln learner
	ln.init(3)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("a", 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("c", 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestLearnsATheBestOfTwoValuesInSameRound(t *testing.T) {
	var ln learner
	ln.init(2)

	ln.Deliver(newVoteFrom("a", 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("c", 1, "bar"))
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("b", 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestBringsOrderOutOfChaos(t *testing.T) {
	var ln learner
	ln.init(2)

	ln.Deliver(newVoteFrom("a", 1, "bar")) //valid
	assert.Equal(t, false, ln.done)
	ln.Deliver(newVoteFrom("c", 2, "funk")) //reset
	assert.Equal(t, false, ln.done)
	ln.Deliver(newVoteFrom("b", 1, "bar")) //ignored
	assert.Equal(t, false, ln.done)

	ln.Deliver(newVoteFrom("c", 1, "foo")) //ignored
	assert.Equal(t, false, ln.done)
	ln.Deliver(newVoteFrom("b", 2, "foo")) //valid
	assert.Equal(t, false, ln.done)
	ln.Deliver(newVoteFrom("a", 2, "foo")) //valid (at quorum)
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}


func TestLearnerIgnoresBadMessages(t *testing.T) {
	var ln learner

	ln.Deliver(Packet{})
	assert.Equal(t, false, ln.done)

	ln.Deliver(Packet{M: M{WireCmd: vote}}) // missing Vrnd
	assert.Equal(t, false, ln.done)
}


func TestSinkLearnsAValue(t *testing.T) {
	var ln learner

	ln.Deliver(Packet{M: *newLearn("foo")})
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestSinkLearnsOkSticky(t *testing.T) {
	var ln learner

	ln.Deliver(Packet{M: *newLearn("foo")})
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)

	ln.Deliver(Packet{M: *newLearn("bar")})
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}
