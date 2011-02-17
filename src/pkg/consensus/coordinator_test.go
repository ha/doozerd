package consensus

import (
	"github.com/bmizerany/assert"
	"testing"
)


func TestCoordIgnoreOldMessages(t *testing.T) {
	var got M

	co := coordinator{
		crnd: 0,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	co.Deliver(Packet{M:*msgTick}) // force the start of a new round

	// The two lines above modify got,
	// so we have to reset it here.
	got = M{}

	co.Deliver(newRsvpFrom("1", 1, 0, ""))
	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	co.Deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, M{}, got)
}


func TestCoordStart(t *testing.T) {
	var got M
	co := coordinator{
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})
	assert.Equal(t, newInvite(1), &got)
}


func TestCoordQuorum(t *testing.T) {
	var got M

	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	got = M{}
	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, M{}, got)
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, M{}, got)
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, M{}, got)
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, M{}, got)
	co.Deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, M{}, got)
}

func TestCoordDuplicateRsvp(t *testing.T) {
	var got M

	co := coordinator{
		size: 10,
		quor: 6,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	got = M{}

	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("6", 1, 0, "")) // from 6
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("6", 1, 0, "")) // from 6
	assert.Equal(t, M{}, got)
}

func TestCoordTargetNomination(t *testing.T) {
	var got M

	co := coordinator{
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	co.Deliver(newRsvpFrom("6", 1, 0, ""))
	co.Deliver(newRsvpFrom("7", 1, 0, ""))

	assert.Equal(t, newNominate(1, "foo"), &got)
}

func TestCoordRetry(t *testing.T) {
	var got M

	co := coordinator{
		size: 10,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	// message from a future round and another proposer
	co.Deliver(newRsvpFrom("2", 2, 0, ""))
	assert.Equal(t, int64(2), co.seen)

	co.Deliver(Packet{M:*msgTick}) // force the start of a new round
	assert.Equal(t, newInvite(11), &got)
}

func TestCoordNonTargetNomination(t *testing.T) {
	var got M

	co := coordinator{
		quor: 6,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	co.Deliver(newRsvpFrom("1", 1, 0, ""))
	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	co.Deliver(newRsvpFrom("6", 1, 1, "bar"))
	assert.Equal(t, newNominate(1, "bar"), &got)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	var got M

	co := coordinator{
		quor: 6,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	// Have to reset this ...
	got = M{}

	co.Deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("6", 1, 0, ""))
	assert.Equal(t, *newNominate(1, "foo"), got)

	got = M{}

	co.Deliver(newRsvpFrom("7", 1, 0, ""))
	assert.Equal(t, M{}, got)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	var got M

	co := coordinator{
		size: 10,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	co.Deliver(newRsvpFrom("1", 1, 0, ""))
	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	co.Deliver(newRsvpFrom("3", 1, 0, ""))
	co.Deliver(newRsvpFrom("4", 1, 0, ""))
	co.Deliver(newRsvpFrom("5", 1, 0, ""))
	co.Deliver(newRsvpFrom("6", 1, 0, ""))

	co.Deliver(Packet{M:*msgTick}) // force the start of a new round

	co.Deliver(newRsvpFrom("1", 11, 0, ""))
	co.Deliver(newRsvpFrom("2", 11, 0, ""))
	co.Deliver(newRsvpFrom("3", 11, 0, ""))
	co.Deliver(newRsvpFrom("4", 11, 0, ""))
	co.Deliver(newRsvpFrom("5", 11, 0, ""))
	co.Deliver(newRsvpFrom("6", 11, 0, ""))

	assert.Equal(t, newNominate(11, "foo"), &got)
}

func TestCoordStartRsvp(t *testing.T) {
	var got M

	co := coordinator{
		quor: 1,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(newRsvpFrom("1", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(Packet{M:*newPropose("foo")})

	// If the RSVPs were ignored, this will be an invite. Otherwise, it'll be a
	// nominate.
	assert.Equal(t, newInvite(1), &got)
}

func TestCoordDuel(t *testing.T) {
	var got M

	co := coordinator{
		quor: 2,
		crnd: 1,
		outs: msgSlot{&got},
	}

	co.Deliver(Packet{M:*newPropose("foo")})

	got = M{}

	co.Deliver(newRsvpFrom("2", 1, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("3", 2, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("4", 2, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("5", 2, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("6", 2, 0, ""))
	assert.Equal(t, M{}, got)

	co.Deliver(newRsvpFrom("7", 2, 0, ""))
	assert.Equal(t, M{}, got)
}


func TestCoordinatorIgnoresBadMessages(t *testing.T) {
	var got M
	co := coordinator{outs: msgSlot{&got}, begun: true}
	c1 := coordinator{outs: msgSlot{&got}, begun: true}


	co.Deliver(Packet{})
	assert.Equal(t, M{}, got)
	assert.Equal(t, c1, co)

	co.Deliver(Packet{M:M{WireCmd: rsvp, Vrnd: new(int64)}}) // missing Crnd
	assert.Equal(t, M{}, got)
	assert.Equal(t, c1, co)

	co.Deliver(Packet{M:M{WireCmd: rsvp, Crnd: new(int64)}}) // missing Vrnd
	assert.Equal(t, M{}, got)
	assert.Equal(t, c1, co)
}
