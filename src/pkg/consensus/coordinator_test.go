package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)


func TestCoordIgnoreOldMessages(t *testing.T) {
	var co coordinator
	co.size = 10

	co.update(&packet{msg: *newPropose("foo")}, -1)

	co.update(&packet{msg: *msgTick}, -1) // force the start of a new round

	got, tick := co.update(newRsvpFrom(0, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(1, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(2, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(3, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(4, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(5, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)
}


func TestCoordStart(t *testing.T) {
	co := coordinator{crnd: 1}

	got, tick := co.update(&packet{msg: *newPropose("foo")}, -1)
	assert.Equal(t, newInvite(1), got)
	assert.Equal(t, true, tick)
}


func TestCoordQuorum(t *testing.T) {
	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
	}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	got, tick := co.update(newRsvpFrom(1, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(2, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(3, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(4, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(5, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)
}

func TestCoordDuplicateRsvp(t *testing.T) {
	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
	}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	got, tick := co.update(newRsvpFrom(1, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(2, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(3, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(4, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(5, 1, 0, "")) // from 5
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(5, 1, 0, "")) // from 5
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)
}

func TestCoordTargetNomination(t *testing.T) {
	co := coordinator{crnd: 1, quor: 6, size: 10}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	co.update(newRsvpFrom(1, 1, 0, ""))
	co.update(newRsvpFrom(2, 1, 0, ""))
	co.update(newRsvpFrom(3, 1, 0, ""))
	co.update(newRsvpFrom(4, 1, 0, ""))
	co.update(newRsvpFrom(5, 1, 0, ""))

	got, tick := co.update(newRsvpFrom(6, 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)
	assert.Equal(t, false, tick)
}

func TestCoordRetry(t *testing.T) {
	co := coordinator{
		size: 10,
		crnd: 1,
	}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	// message from a future round and another proposer
	got, tick := co.update(newRsvpFrom(1, 2, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	// second message from a future round and another proposer
	got, tick = co.update(newRsvpFrom(1, 2, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(&packet{msg: *msgTick}, -1) // force the start of a new round
	assert.Equal(t, newInvite(11), got)
	assert.Equal(t, true, tick)
}


func TestCoordNonTargetNomination(t *testing.T) {
	co := coordinator{
		quor: 6,
		size: 10,
		crnd: 1,
	}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	co.update(newRsvpFrom(0, 1, 0, ""))
	co.update(newRsvpFrom(1, 1, 0, ""))
	co.update(newRsvpFrom(2, 1, 0, ""))
	co.update(newRsvpFrom(3, 1, 0, ""))
	co.update(newRsvpFrom(4, 1, 0, ""))
	got, tick := co.update(newRsvpFrom(5, 1, 1, "bar"))
	assert.Equal(t, newNominate(1, "bar"), got)
	assert.Equal(t, false, tick)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	co := coordinator{
		quor: 6,
		crnd: 1,
		size: 10,
	}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	got, tick := co.update(newRsvpFrom(0, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(1, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(2, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(3, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(4, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(5, 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(6, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	co := coordinator{
		quor: 6,
		size: 10,
		crnd: 1,
	}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	co.update(newRsvpFrom(0, 1, 0, ""))
	co.update(newRsvpFrom(1, 1, 0, ""))
	co.update(newRsvpFrom(2, 1, 0, ""))
	co.update(newRsvpFrom(3, 1, 0, ""))
	co.update(newRsvpFrom(4, 1, 0, ""))
	co.update(newRsvpFrom(5, 1, 0, ""))

	co.update(&packet{msg: *msgTick}, -1) // force the start of a new round

	got, tick := co.update(newRsvpFrom(0, 11, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(1, 11, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(2, 11, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(3, 11, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(4, 11, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(5, 11, 0, ""))
	assert.Equal(t, newNominate(11, "foo"), got)
	assert.Equal(t, false, tick)
}

func TestCoordStartRsvp(t *testing.T) {
	co := coordinator{
		quor: 1,
		crnd: 1,
	}

	got, tick := co.update(newRsvpFrom(0, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(&packet{msg: *newPropose("foo")}, -1)

	// If the RSVPs were ignored, this will be an invite.
	// Otherwise, it'll be a nominate.
	assert.Equal(t, newInvite(1), got)
	assert.Equal(t, true, tick)
}

func TestCoordDuel(t *testing.T) {
	co := coordinator{
		quor: 2,
		size: 3,
		crnd: 1,
	}

	co.update(&packet{msg: *newPropose("foo")}, -1)

	got, tick := co.update(newRsvpFrom(1, 1, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(2, 2, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(3, 2, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(4, 2, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(5, 2, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)

	got, tick = co.update(newRsvpFrom(6, 2, 0, ""))
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)
}


func TestCoordinatorIgnoresBadMessages(t *testing.T) {
	co := coordinator{begun: true}

	// missing Crnd
	got, tick := co.update(&packet{msg: msg{Cmd: rsvp, Vrnd: new(int64)}}, -1)
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)
	assert.Equal(t, coordinator{begun: true}, co)

	// missing Vrnd
	got, tick = co.update(&packet{msg: msg{Cmd: rsvp, Crnd: new(int64)}}, -1)
	assert.Equal(t, (*msg)(nil), got)
	assert.Equal(t, false, tick)
	assert.Equal(t, coordinator{begun: true}, co)
}
