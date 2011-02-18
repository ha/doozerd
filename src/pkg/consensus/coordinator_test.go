package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)


func TestCoordIgnoreOldMessages(t *testing.T) {
	var co coordinator

	co.Deliver(Packet{M: *newPropose("foo")})

	co.Deliver(Packet{M: *msgTick}) // force the start of a new round

	got := co.Deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
}


func TestCoordStart(t *testing.T) {
	co := coordinator{crnd: 1}

	got := co.Deliver(Packet{M: *newPropose("foo")})
	assert.Equal(t, newInvite(1), got)
}


func TestCoordQuorum(t *testing.T) {
	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
	}

	co.Deliver(Packet{M: *newPropose("foo")})

	got := co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	got = co.Deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
}

func TestCoordDuplicateRsvp(t *testing.T) {
	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
	}

	co.Deliver(Packet{M: *newPropose("foo")})

	got := co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("6", 1, 0, "")) // from 6
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("6", 1, 0, "")) // from 6
	assert.Equal(t, (*M)(nil), got)
}

func TestCoordTargetNomination(t *testing.T) {
	co := coordinator{crnd: 1, quor: 6}

	co.Deliver(Packet{M: *newPropose("foo")})

	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	co.Deliver(newRsvpFrom("6", 1, 0, ""))

	got := co.Deliver(newRsvpFrom("7", 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)
}

func TestCoordRetry(t *testing.T) {
	co := coordinator{
		size: 10,
		crnd: 1,
	}

	co.Deliver(Packet{M: *newPropose("foo")})

	// message from a future round and another proposer
	co.Deliver(newRsvpFrom("2", 2, 0, ""))
	assert.Equal(t, int64(2), co.seen)

	got := co.Deliver(Packet{M: *msgTick}) // force the start of a new round
	assert.Equal(t, newInvite(11), got)
}

func TestCoordNonTargetNomination(t *testing.T) {
	co := coordinator{
		quor: 6,
		crnd: 1,
	}

	co.Deliver(Packet{M: *newPropose("foo")})

	co.Deliver(newRsvpFrom("1", 1, 0, ""))
	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	got := co.Deliver(newRsvpFrom("6", 1, 1, "bar"))
	assert.Equal(t, newNominate(1, "bar"), got)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	co := coordinator{
		quor: 6,
		crnd: 1,
	}

	co.Deliver(Packet{M: *newPropose("foo")})

	got := co.Deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)

	got = co.Deliver(newRsvpFrom("7", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	co := coordinator{
		quor: 6,
		size: 10,
		crnd: 1,
	}

	co.Deliver(Packet{M: *newPropose("foo")})

	co.Deliver(newRsvpFrom("1", 1, 0, ""))
	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	co.Deliver(newRsvpFrom("6", 1, 0, ""))

	co.Deliver(Packet{M: *msgTick}) // force the start of a new round

	got := co.Deliver(newRsvpFrom("1", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("2", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("3", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("4", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("5", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("6", 11, 0, ""))
	assert.Equal(t, newNominate(11, "foo"), got)
}

func TestCoordStartRsvp(t *testing.T) {
	co := coordinator{
		quor: 1,
		crnd: 1,
	}

	got := co.Deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(Packet{M: *newPropose("foo")})

	// If the RSVPs were ignored, this will be an invite.
	// Otherwise, it'll be a nominate.
	assert.Equal(t, newInvite(1), got)
}

func TestCoordDuel(t *testing.T) {
	co := coordinator{
		quor: 2,
		crnd: 1,
	}

	co.Deliver(Packet{M: *newPropose("foo")})

	got := co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("3", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("4", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("5", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("6", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)

	got = co.Deliver(newRsvpFrom("7", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
}


func TestCoordinatorIgnoresBadMessages(t *testing.T) {
	co := coordinator{begun: true}

	got := co.Deliver(Packet{})
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, coordinator{begun: true}, co)

	// missing Crnd
	got = co.Deliver(Packet{M: M{WireCmd: rsvp, Vrnd: new(int64)}})
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, coordinator{begun: true}, co)

	// missing Vrnd
	got = co.Deliver(Packet{M: M{WireCmd: rsvp, Crnd: new(int64)}})
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, coordinator{begun: true}, co)
}
