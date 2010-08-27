package paxos

import (
	"borg/util"
)

// In-memory format:
//
//     0      -- index of sender
//     1      -- cmd
//     2..9   -- cluster version
//     10..17 -- seqn
//     18..   -- body -- format depends on command
//
// Wire format is same as in-memory format, but without the first byte (the
// sender index). Here it is for clarity:
//
//     0     -- cmd
//     1..8  -- cluster version
//     9..15 -- seqn
//     16..  -- body -- format depends on command
//
// Here's how you create a `Msg` from incoming network data. This assumes you
// know some upper bound on the size of a message (for instance, UDP packets
// can't ever be more than about 1,500 bytes in practice over Ethernet).
//
// First, allocate a message that's definitely big enough:
//
//     m := make(Msg, 3000) // plenty of space for an Ethernet frame
//
// Then, read in bytes from the wire into the "wire format" portion of the
// Msg:
//
//     n, addr, _ := conn.ReadFrom(m.WireBytes())
//
// Finally, slice off the proper size of the message object:
//
//     m = m[0:n]
//
// Of course, you'll want to do error checking and probably fill in the `From`
// index based on the UDP sender address.
type Msg []byte

const (
	mFrom = iota
	mCmd
	mClusterVersion // 2 - 9
	mSeqn = 10 // - 17
	mBody = 18
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

func newInvite(crnd uint64) Msg {
	m := make(Msg, baseLen + inviteLen)
	m[mCmd] = Invite
	util.Packui64(m.Body()[0:8], crnd)
	return m
}

// Returns the info for `m`. If `m` is not an invite, the result is undefined.
func inviteParts(m Msg) (crnd uint64) {
	return util.Unpackui64(m.Body())
}

func newNominate(crnd uint64, v string) Msg {
	m := make(Msg, baseLen+nominateLen+len(v))
	m[mCmd] = Nominate
	util.Packui64(m.Body()[0:8], crnd)
	copy(m.Body()[nominateLen:], []byte(v))
	return m
}

// Returns the info for `m`. If `m` is not a nominate, the result is undefined.
func nominateParts(m Msg) (crnd uint64, v string) {
	crnd = util.Unpackui64(m.Body()[0:8])
	v = string(m.Body()[8:])
	return
}

func newRsvp(i, vrnd uint64, vval string) Msg {
	m := make(Msg, baseLen+rsvpLen+len(vval))
	m[mCmd] = Rsvp
	util.Packui64(m.Body()[0:8], i)
	util.Packui64(m.Body()[8:16], vrnd)
	copy(m.Body()[rsvpLen:], []byte(vval))
	return m
}

// Returns the info for `m`. If `m` is not an rsvp, the result is undefined.
func rsvpParts(m Msg) (i, vrnd uint64, vval string) {
	i = util.Unpackui64(m.Body()[0:8])
	vrnd = util.Unpackui64(m.Body()[8:16])
	vval = string(m.Body()[16:])
	return
}

func newVote(i uint64, vval string) Msg {
	m := make(Msg, baseLen+voteLen+len(vval))
	m[mCmd] = Vote
	util.Packui64(m.Body()[0:8], i)
	copy(m.Body()[voteLen:], []byte(vval))
	return m
}

// Returns the info for `m`. If `m` is not a vote, the result is undefined.
func voteParts(m Msg) (i uint64, vval string) {
	i = util.Unpackui64(m.Body()[0:8])
	vval = string(m.Body()[8:])
	return
}

func (m Msg) From() int {
	return int(m[mFrom])
}

func (m Msg) Cmd() int {
	return int(m[mCmd])
}

func (m Msg) ClusterVersion() uint64 {
	return util.Unpackui64(m[mClusterVersion : mClusterVersion+8])
}

func (m Msg) Seqn() uint64 {
	return util.Unpackui64(m[mSeqn : mSeqn+8])
}

func (m Msg) Body() []byte {
	return m[mBody:]
}

// Typically used after reading from the network, when building a new `Msg`
// object.
func (m Msg) SetFrom(from byte) {
	m[mFrom] = from
}

// Typically used just before writing `m` to the network.
func (m Msg) SetClusterVersion(ver uint64) {
	util.Packui64(m[mClusterVersion:mClusterVersion+8], ver)
}

// Typically used just before writing `m` to the network.
func (m Msg) SetSeqn(seqn uint64) {
	util.Packui64(m[mSeqn:mSeqn+8], seqn)
}

// Check that `m` is well-formed. Does not guarantee that it will be valid or
// meaningful. If this method returns `true`, you can safely pass `m` into a
// `Putter`.
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

func (m Msg) WireBytes() []byte {
	return m[1:]
}
