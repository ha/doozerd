package paxos

import (
    "borg/assert"
    "testing"
)

func TestLearnsAValueWithAQuorumOfOne(t *testing.T) {
    mess := make(chan string)
    taught := make(chan string)
    exp := "foo"

    go learn(1, mess, taught, func(){})
    mess <- "1:*:VOTE:1:" + exp
    assert.Equal(t, exp, <-taught, "")
}

func TestLearnsAValueWithAQuorumOfTwo(t *testing.T) {
    mess := make(chan string)
    taught := make(chan string)
    exp := "foo"

    go learn(2, mess, taught, func(){})
    mess <- "1:*:VOTE:1:" + exp
    mess <- "2:*:VOTE:1:" + exp
    assert.Equal(t, exp, <-taught, "")
}

func TestIgnoresMalformedMessageMissingSenderId(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(1, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "*:VOTE:1:" + exp
    msgs <- "1:*:VOTE:1:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 1, acks, "")
}

func TestIgnoresMalformedMessageBadRoundNumber(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(1, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "1:*:VOTE:x:" + exp
    msgs <- "1:*:VOTE:1:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 1, acks, "")
}

func TestIgnoresMalformedMessageBadSender(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(1, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "x:*:VOTE:1:" + exp
    msgs <- "1:*:VOTE:1:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 1, acks, "")
}

func TestIgnoresMalformedMessageBadCommand(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(1, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "1:*:foo:1:" + exp
    msgs <- "1:*:VOTE:1:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 1, acks, "")
}

func TestIgnoresMultipleMessagesFromSameSender(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(2, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "1:*:VOTE:1:" + exp
    msgs <- "1:*:VOTE:1:" + exp
    msgs <- "2:*:VOTE:1:" + exp

    assert.Equal(t, exp, <-taught, "")
    // A quick test to make sure all messages were received.  If we get here
    // and it passes without deadlocking, we're all good.
    assert.Equal(t, 3, acks, "")
}

func TestIgnoresSenderInOldRound(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(2, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "1:*:VOTE:2:" + exp
    msgs <- "2:*:VOTE:1:" + exp
    msgs <- "2:*:VOTE:2:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 3, acks, "")
}

func TestResetsVotedFlags(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(2, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "1:*:VOTE:1:" + exp
    msgs <- "1:*:VOTE:2:" + exp
    msgs <- "2:*:VOTE:2:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 3, acks, "")
}

func TestResetsVoteCounts(t *testing.T) {
    msgs := make(chan string)
    taught := make(chan string)
    acks := 0

    exp := "foo"

    go learn(3, msgs, taught, func() { acks++ })
    // Send a message with no senderId
    msgs <- "1:*:VOTE:1:" + exp
    msgs <- "2:*:VOTE:1:" + exp
    msgs <- "3:*:VOTE:2:" + exp
    msgs <- "2:*:VOTE:2:" + exp
    msgs <- "1:*:VOTE:2:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 5, acks, "")
}

func TestLearnsATheBestOfTwoValuesInSameRound(t *testing.T) {
    mess := make(chan string)
    taught := make(chan string)
    exp := "foo"
    acks := 0

    go learn(2, mess, taught, func(){ acks ++ })
    mess <- "1:*:VOTE:1:" + exp
    mess <- "3:*:VOTE:1:" + "bar"
    mess <- "2:*:VOTE:1:" + exp

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 3, acks, "")
}

func TestBringsOrderOutOfChaos(t *testing.T) {
    mess := make(chan string)
    taught := make(chan string)
    exp := "foo"
    acks := 0

    go learn(2, mess, taught, func(){ acks ++ })
    mess <- "1:*:VOTE:1:" + "bar"  //valid
    mess <- "3:*:VOTE:2:" + "funk" //reset
    mess <- "2:*:VOTE:1:" + "bar"  //ignored
    mess <- "3:*:VOTE:1:" + exp    //ignored
    mess <- "2:*:VOTE:2:" + exp    //valid
    mess <- "1:*:VOTE:2:" + exp    //valid (at quorum)

    assert.Equal(t, exp, <-taught, "")
    assert.Equal(t, 6, acks, "")
}
