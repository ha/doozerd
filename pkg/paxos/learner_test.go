package paxos

import (
    "borg/assert"
    "testing"
)

func TestLearnsAValueWithAQuorumOfOne(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(1, msgs)
    }()

    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestLearnsAValueWithAQuorumOfTwo(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(2, msgs)
    }()

    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestIgnoresMalformedMessageBadRoundNumber(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(1, msgs)
    }()

    msgs <- m("1:*:VOTE:x:bar")
    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestIgnoresMalformedMessageBadCommand(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(1, msgs)
    }()

    msgs <- m("1:*:foo:1:bar")
    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestIgnoresMessageWithIncorrectArityInBody(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(1, msgs)
    }()

    msgs <- m("1:*:VOTE:")
    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestIgnoresMultipleMessagesFromSameSender(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(2, msgs)
    }()

    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
    // A quick test to make sure all msgsages were received.  If we get here
    // and it passes without deadlocking, we're all good.
}

func TestIgnoresSenderInOldRound(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(2, msgs)
    }()

    msgs <- m("1:*:VOTE:2:foo")
    msgs <- m("2:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:2:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestResetsVotedFlags(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(2, msgs)
    }()

    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("1:*:VOTE:2:foo")
    msgs <- m("2:*:VOTE:2:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestResetsVoteCounts(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(3, msgs)
    }()

    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:1:foo")
    msgs <- m("1:*:VOTE:2:foo")
    msgs <- m("2:*:VOTE:2:foo")
    msgs <- m("3:*:VOTE:2:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestLearnsATheBestOfTwoValuesInSameRound(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(2, msgs)
    }()

    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("3:*:VOTE:1:bar")
    msgs <- m("2:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestExitsQuietly(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(2, msgs)
    }()

    close(msgs)

    assert.Equal(t, "", <-taught, "")
}

func TestBringsOrderOutOfChaos(t *testing.T) {
    msgs := make(chan Msg)
    taught := make(chan string)

    go func() {
        taught <- learner(2, msgs)
    }()

    msgs <- m("1:*:VOTE:1:bar")  //valid
    msgs <- m("3:*:VOTE:2:funk") //reset
    msgs <- m("2:*:VOTE:1:bar")  //ignored

    msgs <- m("3:*:VOTE:1:foo")    //ignored
    msgs <- m("2:*:VOTE:2:foo")    //valid
    msgs <- m("1:*:VOTE:2:foo")    //valid (at quorum)

    assert.Equal(t, "foo", <-taught, "")
}
