package paxos

import (
	"junta/assert"
	"testing"
)

func TestIgnoreOldMessages(t *testing.T) {
	tests := [][]Msg{
		[]Msg{newInviteFrom(1, 11), newNominateFrom(1, 1, "v")},
		[]Msg{newNominateFrom(1, 11, "v"), newInviteFrom(1, 1)},
		[]Msg{newInviteFrom(1, 11), newInviteFrom(1, 1)},
		[]Msg{newNominateFrom(1, 11, "v"), newNominateFrom(1, 1, "v")},
	}

	for _, test := range tests {
		var got Msg
		ac := acceptor{outs:msgSlot{&got}}

		ac.Put(test[0])

		// We want to check that it didn't try to send a response.
		got = Msg{}
		ac.Put(test[1])
		assert.Equal(t, Msg{}, got)
	}
}

func TestAcceptsInvite(t *testing.T) {
	var got Msg
	ac := acceptor{outs:msgSlot{&got}}
	ac.Put(newInviteFrom(1, 1))
	assert.Equal(t, newRsvp(1, 0, ""), got)
}

func TestItVotes(t *testing.T) {
	totest := [][]Msg{
		[]Msg{newNominateFrom(1, 1, "foo"), newVote(1, "foo")},
		[]Msg{newNominateFrom(1, 1, "bar"), newVote(1, "bar")},
	}

	for _, test := range totest {
		var got Msg
		ac := acceptor{outs:msgSlot{&got}}
		ac.Put(test[0])
		assert.Equal(t, test[1], got, test)
	}
}

func TestItVotesWithAnotherRound(t *testing.T) {
	var got Msg
	ac := acceptor{outs:msgSlot{&got}}
	val := "bar"

	// According to paxos, we can omit Phase 1 in the first round
	ac.Put(newNominateFrom(1, 2, val))
	assert.Equal(t, newVote(2, val), got)
}

func TestItVotesWithAnotherSelf(t *testing.T) {
	var got Msg
	ac := acceptor{outs:msgSlot{&got}}
	val := "bar"

	// According to paxos, we can omit Phase 1 in the first round
	ac.Put(newNominateFrom(1, 2, val))
	assert.Equal(t, newVote(2, val), got)
}

func TestVotedRoundsAndValuesAreTracked(t *testing.T) {
	var got Msg
	ac := acceptor{outs:msgSlot{&got}}

	ac.Put(newNominateFrom(1, 1, "v"))
	ac.Put(newInviteFrom(1, 2))
	assert.Equal(t, newRsvp(2, 1, "v"), got)
}

func TestVotesOnlyOncePerRound(t *testing.T) {
	var got Msg
	ac := acceptor{outs:msgSlot{&got}}

	ac.Put(newNominateFrom(1, 1, "v"))
	assert.Equal(t, newVote(1, "v"), got)

	ac.outs = funcPutter(func(m Msg) {
		t.Error("should not vote twice in one round")
	})
	ac.Put(newNominateFrom(1, 1, "v"))
}
