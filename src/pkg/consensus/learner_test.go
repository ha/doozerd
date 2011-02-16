package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestLearnsAValueWithAQuorumOfOne(t *testing.T) {
	ln := *newLearner(1)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestLearnsOkSticky(t *testing.T) {
	ln := *newLearner(1)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestLearnsAValueWithAQuorumOfTwo(t *testing.T) {
	ln := *newLearner(2)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestIgnoresMalformedMessageBadRoundNumber(t *testing.T) {
	ln := *newLearner(1)

	ln.Put(newVoteFrom(1, 0, "bar"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestIgnoresMultipleMessagesFromSameSender(t *testing.T) {
	ln := *newLearner(2)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestIgnoresSenderInOldRound(t *testing.T) {
	ln := *newLearner(2)

	ln.Put(newVoteFrom(1, 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestResetsVotedFlags(t *testing.T) {
	ln := *newLearner(2)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(1, 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestResetsVoteCounts(t *testing.T) {
	ln := *newLearner(3)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(1, 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 2, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(3, 2, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestLearnsATheBestOfTwoValuesInSameRound(t *testing.T) {
	ln := *newLearner(2)

	ln.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(3, 1, "bar"))
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}

func TestBringsOrderOutOfChaos(t *testing.T) {
	ln := *newLearner(2)

	ln.Put(newVoteFrom(1, 1, "bar")) //valid
	assert.Equal(t, false, ln.done)
	ln.Put(newVoteFrom(3, 2, "funk")) //reset
	assert.Equal(t, false, ln.done)
	ln.Put(newVoteFrom(2, 1, "bar")) //ignored
	assert.Equal(t, false, ln.done)

	ln.Put(newVoteFrom(3, 1, "foo")) //ignored
	assert.Equal(t, false, ln.done)
	ln.Put(newVoteFrom(2, 2, "foo")) //valid
	assert.Equal(t, false, ln.done)
	ln.Put(newVoteFrom(1, 2, "foo")) //valid (at quorum)
	assert.Equal(t, true, ln.done)
	assert.Equal(t, "foo", ln.v)
}
