package paxos

import (
    "borg/assert"
    "testing"
)

// For testing convenience
func newVoteFrom(from byte, i uint64, vval string) Message {
    m := NewVote(i, vval)
    m.SetFrom(from)
    return m
}

// For testing convenience
func newNominateFrom(from byte, crnd int, v string) Message {
    m := NewNominate(crnd, v)
    m.SetFrom(from)
    return m
}

// For testing convenience
func newRsvpFrom(from byte, i, vrnd uint64, vval string) Message {
    m := NewRsvp(i, vrnd, vval)
    m.SetFrom(from)
    return m
}

// For testing convenience
func newInviteFrom(from byte, rnd int) Message {
    m := NewInvite(rnd)
    m.SetSeqn(1)
    m.SetFrom(from)
    return m
}

func TestNewInvite(t *testing.T) {
    m := NewInvite(1)
    assert.Equal(t, "INVITE", m.Cmd(), "")
    assert.Equal(t, "1", m.Body(), "")
}

func TestNewInviteAlt(t *testing.T) {
    m := NewInvite(2)
    assert.Equal(t, "INVITE", m.Cmd(), "")
    assert.Equal(t, "2", m.Body(), "")
}

func TestNewNominate(t *testing.T) {
    m := NewNominate(1, "foo")
    assert.Equal(t, "NOMINATE", m.Cmd(), "")
    assert.Equal(t, "1:foo", m.Body(), "")
}

func TestNewNominateAlt(t *testing.T) {
    m := NewNominate(2, "bar")
    assert.Equal(t, "NOMINATE", m.Cmd(), "")
    assert.Equal(t, "2:bar", m.Body(), "")
}

func TestNewRsvp(t *testing.T) {
    m := NewRsvp(1, 0, "")
    assert.Equal(t, "RSVP", m.Cmd(), "")
    assert.Equal(t, "1:0:", m.Body(), "")
}

func TestNewRsvpAlt(t *testing.T) {
    m := NewRsvp(2, 1, "foo")
    assert.Equal(t, "RSVP", m.Cmd(), "")
    assert.Equal(t, "2:1:foo", m.Body(), "")
}

func TestNewVote(t *testing.T) {
    m := NewVote(1, "foo")
    assert.Equal(t, "VOTE", m.Cmd(), "")
    assert.Equal(t, "1:foo", m.Body(), "")
}

func TestNewVoteAlt(t *testing.T) {
    m := NewVote(2, "bar")
    assert.Equal(t, "VOTE", m.Cmd(), "")
    assert.Equal(t, "2:bar", m.Body(), "")
}

func TestSetFrom(t *testing.T) {
    m := NewInvite(1)
    m.SetFrom(1)
    assert.Equal(t, uint64(1), m.From(), "")
    m.SetFrom(2)
    assert.Equal(t, uint64(2), m.From(), "")
}

func TestSetSeqn(t *testing.T) {
    m := NewInvite(1)
    m.SetSeqn(1)
    assert.Equal(t, uint64(1), m.Seqn(), "")
    m.SetSeqn(2)
    assert.Equal(t, uint64(2), m.Seqn(), "")
}
