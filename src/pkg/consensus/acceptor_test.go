package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestIgnoreOldMessages(t *testing.T) {
	tests := [][]*M{
		{newInviteFrom(1, 11), newNominateFrom(1, 1, "v")},
		{newNominateFrom(1, 11, "v"), newInviteFrom(1, 1)},
		{newInviteFrom(1, 11), newInviteFrom(1, 1)},
		{newNominateFrom(1, 11, "v"), newNominateFrom(1, 1, "v")},
	}

	for _, test := range tests {
		var got M
		ac := acceptor{outs: msgSlot{&got}}

		ac.Put(test[0])

		// We want to check that it didn't try to send a response.
		got = M{}
		ac.Put(test[1])
		assert.Equal(t, M{}, got)
	}
}

func TestAcceptsInvite(t *testing.T) {
	var got M
	ac := acceptor{outs: msgSlot{&got}}
	ac.Put(newInviteFrom(1, 1))
	assert.Equal(t, newRsvp(1, 0, ""), &got)
}

func TestItVotes(t *testing.T) {
	totest := [][]*M{
		{newNominateFrom(1, 1, "foo"), newVote(1, "foo")},
		{newNominateFrom(1, 1, "bar"), newVote(1, "bar")},
	}

	for _, test := range totest {
		var got M
		ac := acceptor{outs: msgSlot{&got}}
		ac.Put(test[0])
		assert.Equal(t, test[1], &got, test)
	}
}

func TestItVotesWithAnotherRound(t *testing.T) {
	var got M
	ac := acceptor{outs: msgSlot{&got}}
	val := "bar"

	// According to paxos, we can omit Phase 1 in the first round
	ac.Put(newNominateFrom(1, 2, val))
	assert.Equal(t, newVote(2, val), &got)
}

func TestItVotesWithAnotherSelf(t *testing.T) {
	var got M
	ac := acceptor{outs: msgSlot{&got}}
	val := "bar"

	// According to paxos, we can omit Phase 1 in the first round
	ac.Put(newNominateFrom(1, 2, val))
	assert.Equal(t, newVote(2, val), &got)
}

func TestVotedRoundsAndValuesAreTracked(t *testing.T) {
	var got M
	ac := acceptor{outs: msgSlot{&got}}

	ac.Put(newNominateFrom(1, 1, "v"))
	ac.Put(newInviteFrom(1, 2))
	assert.Equal(t, newRsvp(2, 1, "v"), &got)
}

func TestVotesOnlyOncePerRound(t *testing.T) {
	var got M
	ac := acceptor{outs: msgSlot{&got}}

	ac.Put(newNominateFrom(1, 1, "v"))
	assert.Equal(t, newVote(1, "v"), &got)

	ac.outs = funcPutter(func(m *M) {
		t.Error("should not vote twice in one round")
	})
	ac.Put(newNominateFrom(1, 1, "v"))
}


func TestAcceptorIgnoresBadMessages(t *testing.T) {
	var got M
	ac := acceptor{outs: msgSlot{&got}}

	ac.Put(&M{})
	assert.Equal(t, M{}, got)

	ac.Put(&M{WireCmd: invite}) // missing Crnd
	assert.Equal(t, M{}, got)

	ac.Put(&M{WireCmd: nominate}) // missing Crnd
	assert.Equal(t, M{}, got)
}
