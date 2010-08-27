package paxos

import (
	"borg/util"
)

const (
	mFrom = iota
	mCmd
	mSeqn // 2-9
	mBody = 10
	baseLen = mBody
)

const (
	Nop = iota
	Invite
	Rsvp
	Nominate
	Vote
)

const (
	inviteLen   = 8
	rsvpLen     = 16 // not including v
	nominateLen = 8  // not including v
	voteLen     = 8  // not including v
)

func NewMessage(b []byte) Msg {
	return Msg(b)
}

func NewInvite(crnd uint64) Msg {
	m := make(Msg, baseLen + inviteLen)
	m[mCmd] = Invite
	util.Packui64(m.Body()[0:8], crnd)
	return m
}

// Returns the info for `m`. If `m` is not an invite, the result is undefined.
func InviteParts(m Msg) (crnd uint64) {
	return util.Unpackui64(m.Body())
}

func NewNominate(crnd uint64, v string) Msg {
	m := make(Msg, baseLen+nominateLen+len(v))
	m[mCmd] = Nominate
	util.Packui64(m.Body()[0:8], crnd)
	copy(m.Body()[nominateLen:], []byte(v))
	return m
}

// Returns the info for `m`. If `m` is not a nominate, the result is undefined.
func NominateParts(m Msg) (crnd uint64, v string) {
	crnd = util.Unpackui64(m.Body()[0:8])
	v = string(m.Body()[8:])
	return
}

func NewRsvp(i, vrnd uint64, vval string) Msg {
	m := make(Msg, baseLen+rsvpLen+len(vval))
	m[mCmd] = Rsvp
	util.Packui64(m.Body()[0:8], i)
	util.Packui64(m.Body()[8:16], vrnd)
	copy(m.Body()[rsvpLen:], []byte(vval))
	return m
}

// Returns the info for `m`. If `m` is not an rsvp, the result is undefined.
func RsvpParts(m Msg) (i, vrnd uint64, vval string) {
	i = util.Unpackui64(m.Body()[0:8])
	vrnd = util.Unpackui64(m.Body()[8:16])
	vval = string(m.Body()[16:])
	return
}

func NewVote(i uint64, vval string) Msg {
	m := make(Msg, baseLen+voteLen+len(vval))
	m[mCmd] = Vote
	util.Packui64(m.Body()[0:8], i)
	copy(m.Body()[voteLen:], []byte(vval))
	return m
}

// Returns the info for `m`. If `m` is not a vote, the result is undefined.
func VoteParts(m Msg) (i uint64, vval string) {
	i = util.Unpackui64(m.Body()[0:8])
	vval = string(m.Body()[8:])
	return
}

// In-memory format:
//
//     0    -- index of sender
//     1    -- cmd
//     2..9 -- seqn
//     10.. -- body -- format depends on command
//
// Wire format is same as in-memory format, but without the first byte (the
// sender index). Here it is for clarity:
//
//     0    -- cmd
//     1..8 -- seqn
//     9..  -- body -- format depends on command
type Msg []byte

func (m Msg) From() int {
	return int(m[mFrom])
}

func (m Msg) Cmd() int {
	return int(m[mCmd])
}

func (m Msg) Seqn() uint64 {
	return util.Unpackui64(m[mSeqn : mSeqn+8])
}

func (m Msg) Body() []byte {
	return m[mBody:]
}

func (m Msg) SetFrom(from byte) {
	m[mFrom] = from
}

func (m Msg) SetSeqn(seqn uint64) {
	util.Packui64(m[mSeqn:mSeqn+8], seqn)
}

func (m Msg) Ok() bool {
	if len(m) < 2 {
		return false
	}
	switch m.Cmd() {
	case Invite:
		return len(m.Body()) == inviteLen
	case Rsvp:
		return len(m.Body()) >= rsvpLen
	case Nominate:
		return len(m.Body()) >= nominateLen
	case Vote:
		return len(m.Body()) >= voteLen
	}
	return false
}
