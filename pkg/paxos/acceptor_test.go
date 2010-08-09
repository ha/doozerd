package paxos

import (
	"borg/assert"
	"testing"
	"fmt"
)

func TestIgnoreOldMessages(t *testing.T) {
	tests := [][]msg{
		msgs("1:*:INVITE:11", "1:*:NOMINATE:1:v"),
		msgs("1:*:NOMINATE:11:v", "1:*:INVITE:1"),
		msgs("1:*:INVITE:11", "1:*:INVITE:1"),
		msgs("1:*:NOMINATE:11:v", "1:*:NOMINATE:1:v"),
	}

	for _, test := range tests {
		ins := make(chan msg)
		outs := make(chan msg)

		go acceptor(2, ins, outs)
		ins <- test[0]
		<-outs // throw away first reply
		ins <- test[1]
		close(ins)

		// outs was closed; therefore all messages have been processed
		assert.Equal(t, []msg{}, gather(outs), fmt.Sprintf("%v", test))
	}
}

func TestAcceptsInvite(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go acceptor(2, ins, outs)
	ins <- m("1:*:INVITE:1")
	close(ins)

	exp := msgs("2:1:RSVP:1:0:")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestIgnoresMalformedMessages(t *testing.T) {
	totest := msgs(
		// TODO: Move to router tests
		//m("x"),            // too few separators
		//m("x:x"),          // too few separators
		//m("x:x:x"),        // too few separators
		//m("x:x:x:x:x"),    // too many separators
		//m("1:x:INVITE:1"), // invalid to address
		//m("X:*:INVITE:1"), // invalid from address
		// TODO: END

		"1:*:INVITE:x", // invalid round number
		"1:*:x:1",      // unknown command
		"1:7:INVITE:1", // valid but incorrect to address

		"1:*:NOMINATE:x",     // too few separators in nominate body
		"1:*:NOMINATE:x:foo", // invalid round number
	)

	for _, test := range totest {
		ins := make(chan msg)
		outs := make(chan msg)

		go acceptor(2, ins, outs)
		ins <- test
		close(ins)

		exp := []msg{}

		// outs was closed; therefore all messages have been processed
		assert.Equal(t, exp, gather(outs), fmt.Sprintf("%v", test))
	}
}

func TestItVotes(t *testing.T) {
	totest := [][]msg{
		msgs("1:*:NOMINATE:1:foo", "2:*:VOTE:1:foo"),
		msgs("1:*:NOMINATE:1:bar", "2:*:VOTE:1:bar"),
	}

	for _, test := range totest {
		ins := make(chan msg)
		outs := make(chan msg)

		go acceptor(2, ins, outs)
		ins <- test[0]
		close(ins)

		// outs was closed; therefore all messages have been processed
		assert.Equal(t, []msg{test[1]}, gather(outs), fmt.Sprintf("%v", test))
	}
}

func TestItVotesWithAnotherRound(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	val := "bar"

	go acceptor(2, ins, outs)
	// According to paxos, we can omit Phase 1 in the first round
	ins <- m("1:*:NOMINATE:2:"+val)
	close(ins)

	exp := msgs("2:*:VOTE:2:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestItVotesWithAnotherSelf(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	val := "bar"

	go acceptor(3, ins, outs)
	// According to paxos, we can omit Phase 1 in the first round
	ins <- m("1:*:NOMINATE:2:"+val)
	close(ins)

	exp := msgs("3:*:VOTE:2:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestVotedRoundsAndValuesAreTracked(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go acceptor(2, ins, outs)
	ins <- m("1:*:NOMINATE:1:v")
	<-outs // throw away VOTE message
	ins <- m("1:*:INVITE:2")
	close(ins)

	exp := msgs("2:1:RSVP:2:1:v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestVotesOnlyOncePerRound(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go acceptor(2, ins, outs)
	ins <- m("1:*:NOMINATE:1:v")
	ins <- m("1:*:NOMINATE:1:v")
	close(ins)

	exp := msgs("2:*:VOTE:1:v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}
