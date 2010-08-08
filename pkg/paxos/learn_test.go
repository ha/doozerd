package paxos

import (
    "borg/assert"
    "testing"
)

func TestLearnsAValueWithAQuorumOfOne(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)

    go learn(1, msgs, taught, func(){})

    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestLearnsAValueWithAQuorumOfTwo(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)

    go learn(2, msgs, taught, func(){})

    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
}

func TestIgnoresMalformedMessageBadRoundNumber(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(1, msgs, taught, func() { acks++ })

    // Send a msgsage with no senderId
    msgs <- m("1:*:VOTE:x:foo")
    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 1, acks, "")
}

func TestIgnoresMalformedMessageBadCommand(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(1, msgs, taught, func() { acks++ })

    // Send a msgsage with no senderId
    msgs <- m("1:*:foo:1:foo")
    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 1, acks, "")
}

func TestIgnoresMessageWithIncorrectArityInBody(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(1, msgs, taught, func() { acks++ })

    // Send a message with no senderId
    msgs <- m("1:*:VOTE:")
    msgs <- m("1:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 1, acks, "")
}

func TestIgnoresMultipleMessagesFromSameSender(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(2, msgs, taught, func() { acks++ })

    // Send a msgsage with no senderId
    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
    // A quick test to make sure all msgsages were received.  If we get here
    // and it passes without deadlocking, we're all good.
    assert.Equal(t, 3, acks, "")
}

func TestIgnoresSenderInOldRound(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(2, msgs, taught, func() { acks++ })

    // Send a msgsage with no senderId
    msgs <- m("1:*:VOTE:2:foo")
    msgs <- m("2:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:2:foo")

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 3, acks, "")
}

func TestResetsVotedFlags(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(2, msgs, taught, func() { acks++ })

    // Send a msgsage with no senderId
    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("1:*:VOTE:2:foo")
    msgs <- m("2:*:VOTE:2:foo")

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 3, acks, "")
}

func TestResetsVoteCounts(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(3, msgs, taught, func() { acks++ })

    // Send a msgsage with no senderId
    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("2:*:VOTE:1:foo")
    msgs <- m("3:*:VOTE:2:foo")
    msgs <- m("2:*:VOTE:2:foo")
    msgs <- m("1:*:VOTE:2:foo")

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 5, acks, "")
}

func TestLearnsATheBestOfTwoValuesInSameRound(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(2, msgs, taught, func(){ acks ++ })

    msgs <- m("1:*:VOTE:1:foo")
    msgs <- m("3:*:VOTE:1:bar")
    msgs <- m("2:*:VOTE:1:foo")

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 3, acks, "")
}

func TestBringsOrderOutOfChaos(t *testing.T) {
    msgs := make(chan msg)
    taught := make(chan string)
    acks := 0

    go learn(2, msgs, taught, func(){ acks ++ })

    msgs <- m("1:*:VOTE:1:bar")  //valid
    msgs <- m("3:*:VOTE:2:funk") //reset
    msgs <- m("2:*:VOTE:1:bar")  //ignored

    msgs <- m("3:*:VOTE:1:foo")    //ignored
    msgs <- m("2:*:VOTE:2:foo")    //valid
    msgs <- m("1:*:VOTE:2:foo")    //valid (at quorum)

    assert.Equal(t, "foo", <-taught, "")
    assert.Equal(t, 6, acks, "")
}
