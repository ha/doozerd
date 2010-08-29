package paxos

import (
	"borg/assert"
	"testing"
	"fmt"
)

func TestIgnoreOldMessages(t *testing.T) {
	tests := [][]Msg{
		[]Msg{newInviteFrom(1, 11), newNominateFrom(1, 1, "v")},
		[]Msg{newNominateFrom(1, 11, "v"), newInviteFrom(1, 1)},
		[]Msg{newInviteFrom(1, 11), newInviteFrom(1, 1)},
		[]Msg{newNominateFrom(1, 11, "v"), newNominateFrom(1, 1, "v")},
	}

	for _, test := range tests {
		ins := make(chan Msg)
		outs := SyncPutter(make(chan Msg))

		go acceptor(ins, outs)
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
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))

	go acceptor(ins, outs)
	ins <- newInviteFrom(1, 1)
	close(ins)

	exp := newRsvp(1, 0, "")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
}

func TestItVotes(t *testing.T) {
	totest := [][]Msg{
		[]Msg{newNominateFrom(1, 1, "foo"), newVote(1, "foo")},
		[]Msg{newNominateFrom(1, 1, "bar"), newVote(1, "bar")},
	}

	for _, test := range totest {
		ins := make(chan Msg)
		outs := SyncPutter(make(chan Msg))

		go acceptor(ins, outs)
		ins <- test[0]
		close(ins)

		// outs was closed; therefore all messages have been processed
		assert.Equal(t, test[1], <-outs, fmt.Sprintf("%v", test))
	}
}

func TestItVotesWithAnotherRound(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))

	val := "bar"

	go acceptor(ins, outs)
	// According to paxos, we can omit Phase 1 in the first round
	ins <- newNominateFrom(1, 2, val)
	close(ins)

	exp := newVote(2, val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
}

func TestItVotesWithAnotherSelf(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))

	val := "bar"

	go acceptor(ins, outs)
	// According to paxos, we can omit Phase 1 in the first round
	ins <- newNominateFrom(1, 2, val)
	close(ins)

	exp := newVote(2, val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
}

func TestVotedRoundsAndValuesAreTracked(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))

	go acceptor(ins, outs)
	ins <- newNominateFrom(1, 1, "v")
	<-outs // throw away VOTE message
	ins <- newInviteFrom(1, 2)
	close(ins)

	exp := newRsvp(2, 1, "v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, <-outs, "")
}

func TestVotesOnlyOncePerRound(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))

	go acceptor(ins, outs)
	ins <- newNominateFrom(1, 1, "v")
	got := <-outs
	ins <- newNominateFrom(1, 1, "v")

	// We want to check that it didn't try to send a response.
	// If it didn't, it will continue to read the next input message and
	// this will work fine. If it did, this will deadlock.
	ins <- newInvite(0) // any old Msg will do here

	close(ins)

	exp := newVote(1, "v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, got, "")
}
