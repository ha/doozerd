package paxos

import (
	"borg/assert"
	"testing"
)

// For testing convenience
func newVoteFrom(from byte, i uint64, vval string) Msg {
	m := newVote(i, vval)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newNominateFrom(from byte, crnd uint64, v string) Msg {
	m := newNominate(crnd, v)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newRsvpFrom(from byte, i, vrnd uint64, vval string) Msg {
	m := newRsvp(i, vrnd, vval)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newInviteFrom(from byte, rnd uint64) Msg {
	m := newInvite(rnd)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

func TestMessageNewInvite(t *testing.T) {
	m := newInvite(1)
	assert.Equal(t, invite, m.Cmd(), "")
	crnd := inviteParts(m)
	assert.Equal(t, uint64(1), crnd, "")
}

func TestMessageNewInviteAlt(t *testing.T) {
	m := newInvite(2)
	assert.Equal(t, invite, m.Cmd(), "")
	crnd := inviteParts(m)
	assert.Equal(t, uint64(2), crnd, "")
}

func TestMessageNewNominate(t *testing.T) {
	m := newNominate(1, "foo")
	assert.Equal(t, nominate, m.Cmd(), "")
	crnd, v := nominateParts(m)
	assert.Equal(t, uint64(1), crnd, "")
	assert.Equal(t, "foo", v, "")
}

func TestMessageNewNominateAlt(t *testing.T) {
	m := newNominate(2, "bar")
	assert.Equal(t, nominate, m.Cmd(), "")
	crnd, v := nominateParts(m)
	assert.Equal(t, uint64(2), crnd, "")
	assert.Equal(t, "bar", v, "")
}

func TestMessageNewRsvp(t *testing.T) {
	m := newRsvp(1, 0, "")
	assert.Equal(t, rsvp, m.Cmd(), "")
	i, vrnd, vval := rsvpParts(m)
	assert.Equal(t, uint64(1), i, "")
	assert.Equal(t, uint64(0), vrnd, "")
	assert.Equal(t, "", vval, "")
}

func TestMessageNewRsvpAlt(t *testing.T) {
	m := newRsvp(2, 1, "foo")
	assert.Equal(t, rsvp, m.Cmd(), "")
	i, vrnd, vval := rsvpParts(m)
	assert.Equal(t, uint64(2), i, "")
	assert.Equal(t, uint64(1), vrnd, "")
	assert.Equal(t, "foo", vval, "")
}

func TestMessageNewVote(t *testing.T) {
	m := newVote(1, "foo")
	assert.Equal(t, vote, m.Cmd(), "")
	i, vval := voteParts(m)
	assert.Equal(t, uint64(1), i, "")
	assert.Equal(t, "foo", vval, "")
}

func TestMessageNewVoteAlt(t *testing.T) {
	m := newVote(2, "bar")
	assert.Equal(t, vote, m.Cmd(), "")
	i, vval := voteParts(m)
	assert.Equal(t, uint64(2), i, "")
	assert.Equal(t, "bar", vval, "")
}

func TestMessageSetFrom(t *testing.T) {
	m := newInvite(1)
	m.SetFrom(1)
	assert.Equal(t, 1, m.From(), "")
	m.SetFrom(2)
	assert.Equal(t, 2, m.From(), "")
}

func TestMessageSetSeqn(t *testing.T) {
	m := newInvite(1)
	m.SetSeqn(1)
	assert.Equal(t, uint64(1), m.Seqn(), "")
	m.SetSeqn(2)
	assert.Equal(t, uint64(2), m.Seqn(), "")
}

func TestMessageSetClusterVersion(t *testing.T) {
	m := newInvite(1)
	m.SetClusterVersion(1)
	assert.Equal(t, uint64(1), m.ClusterVersion(), "")
	m.SetClusterVersion(2)
	assert.Equal(t, uint64(2), m.ClusterVersion(), "")
}

func resize(m Msg, n int) Msg {
	x := len(m)+n
	if x > cap(m) {
		y := make(Msg, x)
		copy(y, m)
		return y
	}
	return m[0:len(m)+n]
}

var badMessages = []Msg{
	Msg{0},      // too short
	Msg{0, 255}, // bad cmd
	resize(newInvite(0), -1), // too short for type
	resize(newInvite(0), 1), // too long for type
	resize(newRsvp(0, 0, ""), -1), // too short for type
	resize(newNominate(0, ""), -1), // too short for type
	resize(newVote(0, ""), -1), // too short for type
}

func TestBadMessagesOk(t *testing.T) {
	for _, m := range badMessages {
		if m.Ok() {
			t.Errorf("check failed for bad msg: %#v", m)
		}
	}
}

var goodMessages = []Msg{
	newInvite(1),
	newRsvp(2, 1, "foo"),
	newNominate(1, "foo"),
	newVote(1, "foo"),
}

func TestGoodMessagesOk(t *testing.T) {
	for _, m := range goodMessages {
		if !m.Ok() {
			t.Errorf("check failed for good msg: %#v", m)
		}
	}
}

func TestWireBytes(t *testing.T) {
	m := newInvite(1)
	b := m.WireBytes()
	assert.Equal(t, []byte(m[1:]), b, "")
	b[0] = 2
	assert.Equal(t, byte(2), m[1], "")
}
