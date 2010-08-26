package paxos

import (
    "borg/assert"
    "testing"
)

// TODO: test wire format
// "x"            // too few separators
// "x:x"          // too few separators
// "x:x:x"        // too few separators
// "x:x:x:x:x"    // too many separators
// "1:x:INVITE:1" // invalid to address
// "X:*:INVITE:1" // invalid from address

//func TestIgnoresMalformedMessages(t *testing.T) {
//	unknownCommand := newInviteFrom(1, 1)
//	unknownCommand.(*Msg).cmd = "x"
//	invalidBody := newNominateFrom(1, 1, "foo")
//	invalidBody.(*Msg).body = "x"
//
//	totest := []Message{
//		newInviteFrom(1, 0), // invalid round number
//		unknownCommand,      // unknown command
//
//		invalidBody,                  // too few separators in nominate body
//		newNominateFrom(1, 0, "foo"), // invalid round number
//	}
//
//	for _, test := range totest {
//		ins := make(chan Message)
//		outs := SyncPutter(make(chan Message))
//
//		go acceptor(ins, PutWrapper{1, 2, outs})
//		ins <- test
//
//		// We want to check that it didn't try to send a response.
//		// If it didn't, it will continue to read the next input message and
//		// this will work fine. If it did, this will deadlock.
//		ins <- test
//
//		close(ins)
//	}
//}

//func TestIgnoresMalformedMessageBadCommand(t *testing.T) {
//    msgs := make(chan Message)
//    taught := make(chan string)
//
//    go func() {
//        taught <- learner(1, msgs)
//    }()
//
//    m := newVoteFrom(1, 1, "foo")
//    m.(*Msg).cmd = "foo"
//    msgs <- m
//    msgs <- newVoteFrom(1, 1, "foo")
//
//    assert.Equal(t, "foo", <-taught, "")
//}
//
//func TestIgnoresMessageWithIncorrectArityInBody(t *testing.T) {
//    msgs := make(chan Message)
//    taught := make(chan string)
//
//    go func() {
//        taught <- learner(1, msgs)
//    }()
//
//    m := newVoteFrom(1, 1, "foo")
//    m.(*Msg).body = ""
//    msgs <- m
//    msgs <- newVoteFrom(1, 1, "foo")
//
//    assert.Equal(t, "foo", <-taught, "")
//}

// For testing convenience
func newVoteFrom(from byte, i uint64, vval string) Message {
    m := NewVote(i, vval)
    m.SetSeqn(1)
    m.SetFrom(from)
    return m
}

// For testing convenience
func newNominateFrom(from byte, crnd int, v string) Message {
    m := NewNominate(crnd, v)
    m.SetSeqn(1)
    m.SetFrom(from)
    return m
}

// For testing convenience
func newRsvpFrom(from byte, i, vrnd uint64, vval string) Message {
    m := NewRsvp(i, vrnd, vval)
    m.SetSeqn(1)
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

func TestInviteParts(t *testing.T) {
    m := NewInvite(1)
    crnd := InviteParts(m)
    assert.Equal(t, 1, crnd, "")
}

func TestNominateParts(t *testing.T) {
    m := NewNominate(1, "foo")
    crnd, v := NominateParts(m)
    assert.Equal(t, 1, crnd, "")
    assert.Equal(t, "foo", v, "")
}

func TestRsvpParts(t *testing.T) {
    m := NewRsvp(1, 0, "")
    i, vrnd, vval := RsvpParts(m)
    assert.Equal(t, uint64(1), i, "")
    assert.Equal(t, uint64(0), vrnd, "")
    assert.Equal(t, "", vval, "")
}

func TestVoteParts(t *testing.T) {
    m := NewVote(1, "foo")
    i, vval := VoteParts(m)
    assert.Equal(t, uint64(1), i, "")
    assert.Equal(t, "foo", vval, "")
}

func TestInvitePartsAlt(t *testing.T) {
    m := NewInvite(2)
    crnd := InviteParts(m)
    assert.Equal(t, 2, crnd, "")
}

func TestNominatePartsAlt(t *testing.T) {
    m := NewNominate(2, "bar")
    crnd, v := NominateParts(m)
    assert.Equal(t, 2, crnd, "")
    assert.Equal(t, "bar", v, "")
}

func TestRsvpPartsAlt(t *testing.T) {
    m := NewRsvp(2, 1, "foo")
    i, vrnd, vval := RsvpParts(m)
    assert.Equal(t, uint64(2), i, "")
    assert.Equal(t, uint64(1), vrnd, "")
    assert.Equal(t, "foo", vval, "")
}

func TestVotePartsAlt(t *testing.T) {
    m := NewVote(2, "bar")
    i, vval := VoteParts(m)
    assert.Equal(t, uint64(2), i, "")
    assert.Equal(t, "bar", vval, "")
}
