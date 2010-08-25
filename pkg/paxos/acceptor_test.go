package paxos

import (
	"borg/assert"
	"testing"
	"fmt"
)

func TestIgnoreOldMessages(t *testing.T) {
	tests := [][]Message{
		msgs("1:*:INVITE:11", "1:*:NOMINATE:1:v"),
		msgs("1:*:NOMINATE:11:v", "1:*:INVITE:1"),
		msgs("1:*:INVITE:11", "1:*:INVITE:1"),
		msgs("1:*:NOMINATE:11:v", "1:*:NOMINATE:1:v"),
	}

	for _, test := range tests {
		ins := make(chan Message)
		outs := SyncPutter(make(chan Message))

		go acceptor(ins, PutWrapper{1, 2, outs})
		ins <- test[0]
		<-outs // throw away first reply
		ins <- test[1]

		// We want to check that it didn't try to send a response.
		// If it didn't, it will continue to read the next input message and
		// this will work fine. If it did, this will deadlock.
		ins <- test[1]
		// If we get here, it passes.

		close(ins)
	}
}

func TestAcceptsInvite(t *testing.T) {
	ins := make(chan Message)
	outs := SyncPutter(make(chan Message))

	go acceptor(ins, PutWrapper{1, 2, outs})
	ins <- m("1:*:INVITE:1")
	close(ins)

	exp := m("2:1:RSVP:1:0:")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
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

		"1:*:NOMINATE:x",     // too few separators in nominate body
		"1:*:NOMINATE:x:foo", // invalid round number
	)

	for _, test := range totest {
		ins := make(chan Message)
		outs := SyncPutter(make(chan Message))

		go acceptor(ins, PutWrapper{1, 2, outs})
		ins <- test

		// We want to check that it didn't try to send a response.
		// If it didn't, it will continue to read the next input message and
		// this will work fine. If it did, this will deadlock.
		ins <- test

		close(ins)
	}
}

func TestItVotes(t *testing.T) {
	totest := [][]Message{
		msgs("1:*:NOMINATE:1:foo", "2:*:VOTE:1:foo"),
		msgs("1:*:NOMINATE:1:bar", "2:*:VOTE:1:bar"),
	}

	for _, test := range totest {
		ins := make(chan Message)
		outs := SyncPutter(make(chan Message))

		go acceptor(ins, PutWrapper{1, 2, outs})
		ins <- test[0]
		close(ins)

		// outs was closed; therefore all messages have been processed
		assert.Equal(t, test[1], <-outs, fmt.Sprintf("%v", test))
	}
}

func TestItVotesWithAnotherRound(t *testing.T) {
	ins := make(chan Message)
	outs := SyncPutter(make(chan Message))

	val := "bar"

	go acceptor(ins, PutWrapper{1, 2, outs})
	// According to paxos, we can omit Phase 1 in the first round
	ins <- m("1:*:NOMINATE:2:"+val)
	close(ins)

	exp := m("2:*:VOTE:2:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
}

func TestItVotesWithAnotherSelf(t *testing.T) {
	ins := make(chan Message)
	outs := SyncPutter(make(chan Message))

	val := "bar"

	go acceptor(ins, PutWrapper{1, 3, outs})
	// According to paxos, we can omit Phase 1 in the first round
	ins <- m("1:*:NOMINATE:2:"+val)
	close(ins)

	exp := m("3:*:VOTE:2:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
}

func TestVotedRoundsAndValuesAreTracked(t *testing.T) {
	ins := make(chan Message)
	outs := SyncPutter(make(chan Message))

	go acceptor(ins, PutWrapper{1, 2, outs})
	ins <- m("1:*:NOMINATE:1:v")
	<-outs // throw away VOTE message
	ins <- m("1:*:INVITE:2")
	close(ins)

	exp := m("2:1:RSVP:2:1:v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
}

func TestVotesOnlyOncePerRound(t *testing.T) {
	ins := make(chan Message)
	outs := SyncPutter(make(chan Message))

	go acceptor(ins, PutWrapper{1, 2, outs})
	ins <- m("1:*:NOMINATE:1:v")
	got := <-outs
	ins <- m("1:*:NOMINATE:1:v")

	// We want to check that it didn't try to send a response.
	// If it didn't, it will continue to read the next input message and
	// this will work fine. If it did, this will deadlock.
	ins <- m("1:*::")

	close(ins)

	exp := m("2:*:VOTE:1:v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, got, "")
}
