package paxos

import (
	"borg/assert"
	"testing"
)

func TestLearnsAValueWithAQuorumOfOne(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(1, msgs)
	}()

	msgs <- newVoteFrom(1, 1, "foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestLearnsAValueWithAQuorumOfTwo(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(2, msgs)
	}()

	msgs <- newVoteFrom(1, 1, "foo")
	msgs <- newVoteFrom(2, 1, "foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestIgnoresMalformedMessageBadRoundNumber(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(1, msgs)
	}()

	msgs <- newVoteFrom(1, 0, "bar")
	msgs <- newVoteFrom(1, 1, "foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestIgnoresMultipleMessagesFromSameSender(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(2, msgs)
	}()

	msgs <- newVoteFrom(1, 1, "foo")
	msgs <- newVoteFrom(1, 1, "foo")
	msgs <- newVoteFrom(2, 1, "foo")

	assert.Equal(t, "foo", <-taught, "")
	// A quick test to make sure all msgsages were received.  If we get here
	// and it passes without deadlocking, we're all good.
}

func TestIgnoresSenderInOldRound(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(2, msgs)
	}()

	msgs <- newVoteFrom(1, 2, "foo")
	msgs <- newVoteFrom(2, 1, "foo")
	msgs <- newVoteFrom(2, 2, "foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestResetsVotedFlags(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(2, msgs)
	}()

	msgs <- newVoteFrom(1, 1, "foo")
	msgs <- newVoteFrom(1, 2, "foo")
	msgs <- newVoteFrom(2, 2, "foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestResetsVoteCounts(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(3, msgs)
	}()

	msgs <- newVoteFrom(1, 1, "foo")
	msgs <- newVoteFrom(2, 1, "foo")
	msgs <- newVoteFrom(1, 2, "foo")
	msgs <- newVoteFrom(2, 2, "foo")
	msgs <- newVoteFrom(3, 2, "foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestLearnsATheBestOfTwoValuesInSameRound(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(2, msgs)
	}()

	msgs <- newVoteFrom(1, 1, "foo")
	msgs <- newVoteFrom(3, 1, "bar")
	msgs <- newVoteFrom(2, 1, "foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestExitsQuietly(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(2, msgs)
	}()

	close(msgs)

	assert.Equal(t, "", <-taught, "")
}

func TestBringsOrderOutOfChaos(t *testing.T) {
	msgs := make(chan Message)
	taught := make(chan string)

	go func() {
		taught <- learner(2, msgs)
	}()

	msgs <- newVoteFrom(1, 1, "bar")  //valid
	msgs <- newVoteFrom(3, 2, "funk") //reset
	msgs <- newVoteFrom(2, 1, "bar")  //ignored

	msgs <- newVoteFrom(3, 1, "foo") //ignored
	msgs <- newVoteFrom(2, 2, "foo") //valid
	msgs <- newVoteFrom(1, 2, "foo") //valid (at quorum)

	assert.Equal(t, "foo", <-taught, "")
}
