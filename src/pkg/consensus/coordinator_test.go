package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)


func TestCoordIgnoreOldMessages(t *testing.T) {
	var co coordinator
	co.size = 10

	co.deliver(packet{M: *newPropose("foo")})

	co.deliver(packet{M: *msgTick}) // force the start of a new round

	got, tick := co.deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
}


func TestCoordStart(t *testing.T) {
	co := coordinator{crnd: 1}

	got, tick := co.deliver(packet{M: *newPropose("foo")})
	assert.Equal(t, newInvite(1), got)
	assert.Equal(t, false, tick)
}


func TestCoordQuorum(t *testing.T) {
	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	got, tick := co.deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
}

func TestCoordDuplicateRsvp(t *testing.T) {
	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	got, tick := co.deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("6", 1, 0, "")) // from 6
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("6", 1, 0, "")) // from 6
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
}

func TestCoordTargetNomination(t *testing.T) {
	co := coordinator{crnd: 1, quor: 6}

	co.deliver(packet{M: *newPropose("foo")})

	co.deliver(newRsvpFrom("2", 1, 0, ""))
	co.deliver(newRsvpFrom("3", 1, 0, ""))
	co.deliver(newRsvpFrom("4", 1, 0, ""))
	co.deliver(newRsvpFrom("5", 1, 0, ""))
	co.deliver(newRsvpFrom("6", 1, 0, ""))

	got, tick := co.deliver(newRsvpFrom("7", 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)
	assert.Equal(t, false, tick)
}

func TestCoordRetry(t *testing.T) {
	co := coordinator{
		size: 10,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	// message from a future round and another proposer
	got, tick := co.deliver(newRsvpFrom("2", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, true, tick)

	// second message from a future round and another proposer
	got, tick = co.deliver(newRsvpFrom("2", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(packet{M: *msgTick}) // force the start of a new round
	assert.Equal(t, newInvite(11), got)
	assert.Equal(t, false, tick)
}


func TestCoordTickTwice(t *testing.T) {
	co := coordinator{
		size: 10,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	// message from a future round and another proposer
	got, tick := co.deliver(newRsvpFrom("2", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, true, tick)

	got, tick = co.deliver(packet{M: *msgTick}) // force the start of a new round
	assert.Equal(t, newInvite(11), got)
	assert.Equal(t, false, tick)

	// message from a future round and another proposer
	got, tick = co.deliver(newRsvpFrom("2", 12, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, true, tick)
}


func TestCoordNonTargetNomination(t *testing.T) {
	co := coordinator{
		quor: 6,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	co.deliver(newRsvpFrom("1", 1, 0, ""))
	co.deliver(newRsvpFrom("2", 1, 0, ""))
	co.deliver(newRsvpFrom("3", 1, 0, ""))
	co.deliver(newRsvpFrom("4", 1, 0, ""))
	co.deliver(newRsvpFrom("5", 1, 0, ""))
	got, tick := co.deliver(newRsvpFrom("6", 1, 1, "bar"))
	assert.Equal(t, newNominate(1, "bar"), got)
	assert.Equal(t, false, tick)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	co := coordinator{
		quor: 6,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	got, tick := co.deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("7", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	co := coordinator{
		quor: 6,
		size: 10,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	co.deliver(newRsvpFrom("1", 1, 0, ""))
	co.deliver(newRsvpFrom("2", 1, 0, ""))
	co.deliver(newRsvpFrom("3", 1, 0, ""))
	co.deliver(newRsvpFrom("4", 1, 0, ""))
	co.deliver(newRsvpFrom("5", 1, 0, ""))
	co.deliver(newRsvpFrom("6", 1, 0, ""))

	co.deliver(packet{M: *msgTick}) // force the start of a new round

	got, tick := co.deliver(newRsvpFrom("1", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("2", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("3", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("4", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("5", 11, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("6", 11, 0, ""))
	assert.Equal(t, newNominate(11, "foo"), got)
	assert.Equal(t, false, tick)
}

func TestCoordStartRsvp(t *testing.T) {
	co := coordinator{
		quor: 1,
		crnd: 1,
	}

	got, tick := co.deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(packet{M: *newPropose("foo")})

	// If the RSVPs were ignored, this will be an invite.
	// Otherwise, it'll be a nominate.
	assert.Equal(t, newInvite(1), got)
	assert.Equal(t, false, tick)
}

func TestCoordDuel(t *testing.T) {
	co := coordinator{
		quor: 2,
		crnd: 1,
	}

	co.deliver(packet{M: *newPropose("foo")})

	got, tick := co.deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("3", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, true, tick)

	got, tick = co.deliver(newRsvpFrom("4", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("5", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("6", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.deliver(newRsvpFrom("7", 2, 0, ""))
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
}


func TestCoordinatorIgnoresBadMessages(t *testing.T) {
	co := coordinator{begun: true}

	got, tick := co.deliver(packet{})
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
	assert.Equal(t, coordinator{begun: true}, co)

	// missing Crnd
	got, tick = co.deliver(packet{M: M{Cmd: rsvp, Vrnd: new(int64)}})
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
	assert.Equal(t, coordinator{begun: true}, co)

	// missing Vrnd
	got, tick = co.deliver(packet{M: M{Cmd: rsvp, Crnd: new(int64)}})
	assert.Equal(t, (*M)(nil), got)
	assert.Equal(t, false, tick)
	assert.Equal(t, coordinator{begun: true}, co)
}
