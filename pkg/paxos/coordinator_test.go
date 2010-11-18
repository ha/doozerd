package paxos

import (
	"doozer/assert"
	"testing"
)

func TestCoordIgnoreOldMessages(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	co.Put(newTick()) // force the start of a new round

	got = Msg{}
	co.Put(newRsvpFrom(1, 1, 0, ""))
	co.Put(newRsvpFrom(2, 1, 0, ""))
	co.Put(newRsvpFrom(3, 1, 0, ""))
	co.Put(newRsvpFrom(4, 1, 0, ""))
	co.Put(newRsvpFrom(5, 1, 0, ""))
	co.Put(newRsvpFrom(6, 1, 0, ""))
	assert.Equal(t, Msg{}, got)
}

func TestCoordStart(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))
	assert.Equal(t, newInvite(1), got)
}

// This is here mainly for triangulation.  It ensures we're not
// hardcoding crnd.
func TestCoordStartAlt(t *testing.T) {
	var got Msg
	cx := newCluster("c", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))
	assert.Equal(t, newInvite(2), got)
}

func TestCoordQuorum(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	got = nil
	co.Put(newRsvpFrom(2, 1, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(3, 1, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(4, 1, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(5, 1, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(6, 1, 0, ""))
	assert.Equal(t, Msg(nil), got)
}

func TestCoordTargetNomination(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	co.Put(newRsvpFrom(2, 1, 0, ""))
	co.Put(newRsvpFrom(3, 1, 0, ""))
	co.Put(newRsvpFrom(4, 1, 0, ""))
	co.Put(newRsvpFrom(5, 1, 0, ""))
	co.Put(newRsvpFrom(6, 1, 0, ""))
	co.Put(newRsvpFrom(7, 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)
}

func TestCoordRetry(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	// message from a future round and another proposer
	co.Put(newRsvpFrom(2, 2, 0, ""))
	assert.Equal(t, uint64(2), co.seen)

	co.Put(newTick()) // force the start of a new round
	assert.Equal(t, newInvite(11), got)
}

func TestCoordNonTargetNomination(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	co.Put(newRsvpFrom(1, 1, 0, ""))
	co.Put(newRsvpFrom(2, 1, 0, ""))
	co.Put(newRsvpFrom(3, 1, 0, ""))
	co.Put(newRsvpFrom(4, 1, 0, ""))
	co.Put(newRsvpFrom(5, 1, 0, ""))
	co.Put(newRsvpFrom(6, 1, 1, "bar"))
	assert.Equal(t, newNominate(1, "bar"), got)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	co.Put(newRsvpFrom(1, 1, 0, ""))
	co.Put(newRsvpFrom(2, 1, 0, ""))
	co.Put(newRsvpFrom(3, 1, 0, ""))
	co.Put(newRsvpFrom(4, 1, 0, ""))
	co.Put(newRsvpFrom(5, 1, 0, ""))
	co.Put(newRsvpFrom(6, 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), got)

	got = Msg{}
	co.Put(newRsvpFrom(7, 1, 0, ""))
	assert.Equal(t, Msg{}, got)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	co.Put(newRsvpFrom(1, 1, 0, ""))
	co.Put(newRsvpFrom(2, 1, 0, ""))
	co.Put(newRsvpFrom(3, 1, 0, ""))
	co.Put(newRsvpFrom(4, 1, 0, ""))
	co.Put(newRsvpFrom(5, 1, 0, ""))
	co.Put(newRsvpFrom(6, 1, 0, ""))

	co.Put(newTick()) // force the start of a new round

	co.Put(newRsvpFrom(1, 11, 0, ""))
	co.Put(newRsvpFrom(2, 11, 0, ""))
	co.Put(newRsvpFrom(3, 11, 0, ""))
	co.Put(newRsvpFrom(4, 11, 0, ""))
	co.Put(newRsvpFrom(5, 11, 0, ""))
	co.Put(newRsvpFrom(6, 11, 0, ""))

	assert.Equal(t, newNominate(11, "foo"), got)
}

func TestCoordStartRsvp(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newRsvpFrom(1, 1, 0, ""))
	co.Put(newRsvpFrom(2, 1, 0, ""))
	co.Put(newRsvpFrom(3, 1, 0, ""))
	co.Put(newRsvpFrom(4, 1, 0, ""))
	co.Put(newRsvpFrom(5, 1, 0, ""))
	co.Put(newRsvpFrom(6, 1, 0, ""))

	co.Put(newPropose("foo"))

	// If the RSVPs were ignored, this will be an invite. Otherwise, it'll be a
	// nominate.
	assert.Equal(t, newInvite(1), got)
}

func TestCoordDuel(t *testing.T) {
	var got Msg
	cx := newCluster("b", tenNodes, tenIds, nil)
	co := coordinator{cx: cx, crnd: uint64(cx.SelfIndex()), outs: msgSlot{&got}}

	co.Put(newPropose("foo"))

	got = nil
	co.Put(newRsvpFrom(2, 1, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(3, 2, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(4, 2, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(5, 2, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(6, 2, 0, ""))
	assert.Equal(t, Msg(nil), got)
	co.Put(newRsvpFrom(7, 2, 0, ""))
	assert.Equal(t, Msg(nil), got)
}
