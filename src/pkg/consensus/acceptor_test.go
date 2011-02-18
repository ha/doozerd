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
		ac := acceptor{}

		ac.Put(test[0])

		got := ac.Put(test[1])
		assert.Equal(t, (*M)(nil), got)
	}
}

func TestAcceptsInvite(t *testing.T) {
	ac := acceptor{}
	got := ac.Put(newInviteFrom(1, 1))
	assert.Equal(t, newRsvp(1, 0, ""), got)
}

func TestItVotes(t *testing.T) {
	totest := [][]*M{
		{newNominateFrom(1, 1, "foo"), newVote(1, "foo")},
		{newNominateFrom(1, 1, "bar"), newVote(1, "bar")},
	}

	for _, test := range totest {
		ac := acceptor{}
		got := ac.Put(test[0])
		assert.Equal(t, test[1], got, test)
	}
}

func TestItVotesWithAnotherRound(t *testing.T) {
	ac := acceptor{}
	val := "bar"

	// According to paxos, we can omit Phase 1 in the first round
	got := ac.Put(newNominateFrom(1, 2, val))
	assert.Equal(t, newVote(2, val), got)
}

func TestItVotesWithAnotherSelf(t *testing.T) {
	ac := acceptor{}
	val := "bar"

	// According to paxos, we can omit Phase 1 in the first round
	got := ac.Put(newNominateFrom(1, 2, val))
	assert.Equal(t, newVote(2, val), got)
}

func TestVotedRoundsAndValuesAreTracked(t *testing.T) {
	ac := acceptor{}

	ac.Put(newNominateFrom(1, 1, "v"))

	got := ac.Put(newInviteFrom(1, 2))
	assert.Equal(t, newRsvp(2, 1, "v"), got)
}

func TestVotesOnlyOncePerRound(t *testing.T) {
	ac := acceptor{}

	got := ac.Put(newNominateFrom(1, 1, "v"))
	assert.Equal(t, newVote(1, "v"), got)

	got = ac.Put(newNominateFrom(1, 1, "v"))
	assert.Equal(t, (*M)(nil), got)
}


func TestAcceptorIgnoresBadMessages(t *testing.T) {
	ac := acceptor{}

	got := ac.Put(&M{})
	assert.Equal(t, (*M)(nil), got)

	got = ac.Put(&M{Cmd: invite}) // missing Crnd
	assert.Equal(t, (*M)(nil), got)

	got = ac.Put(&M{Cmd: nominate}) // missing Crnd
	assert.Equal(t, (*M)(nil), got)
}
