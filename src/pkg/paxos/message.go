package paxos

import (
	"doozer/util"
	"os"
)

// In-memory format:
//
//     0      -- index of sender
//     1      -- cmd
//     2      -- flags
//     3..10   -- cluster version
//     11..17 -- seqn
//     18..   -- body -- format depends on command
//
// Wire format is same as in-memory format, but without the first byte (the
// sender index). Here it is for clarity:
//
//     0     -- cmd
//     1     -- flags
//     2..9  -- cluster version
//     10..16 -- seqn
//     17..  -- body -- format depends on command
//
// Here's how you create a `Msg` from incoming network data. This assumes you
// know some upper bound on the size of a message (for instance, UDP packets
// can't ever be more than about 1,500 bytes in practice over Ethernet).
//
//     m, addr, err := ReadMsg(conn, 3000) // plenty for an Ethernet frame
//
// Of course, you'll want to do error checking and probably fill in the `From`
// index based on the UDP sender address.
type Msg []byte

const (
	mFrom = iota
	mCmd
	mFlags
	mSeqn
	mSeqn1
	mSeqn2
	mSeqn3
	mSeqn4
	mSeqn5
	mSeqn6
	mSeqn7
	mBody
	baseLen = mBody
)

const (
	nop = iota
	invite
	rsvp
	nominate
	vote
	tick
	propose
	learn
)

// Flags
const (
	Ack = 1 << iota
)

const (
	inviteLen   = 8
	rsvpLen     = 16 // not including v
	nominateLen = 8  // not including v
	voteLen     = 8  // not including v
	tickLen     = 0
	proposeLen  = 0 // not including v
	learnLen    = 0 // not including v
)

var (
	msgTick = newTick()
)

func newInvite(crnd uint64) Msg {
	m := make(Msg, baseLen+inviteLen)
	m[mCmd] = invite
	util.Packui64(m.Body()[0:8], crnd)
	return m
}

// Returns the info for `m`. If `m` is not an invite, the result is undefined.
func inviteParts(m Msg) (crnd uint64) {
	return util.Unpackui64(m.Body())
}

func newNominate(crnd uint64, v string) Msg {
	m := make(Msg, baseLen+nominateLen+len(v))
	m[mCmd] = nominate
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
	m[mCmd] = rsvp
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
	m[mCmd] = vote
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

func newTick() Msg {
	m := make(Msg, baseLen+tickLen)
	m[mCmd] = tick
	return m
}

func newPropose(val string) Msg {
	m := make(Msg, baseLen+proposeLen+len(val))
	m[mCmd] = propose
	copy(m.Body()[proposeLen:], []byte(val))
	return m
}

func newLearn(val string) Msg {
	m := make(Msg, baseLen+learnLen+len(val))
	m[mCmd] = learn
	copy(m.Body()[learnLen:], []byte(val))
	return m
}

func learnParts(m Msg) string {
	return string(m.Body())
}


// Returns the info for `m`. If `m` is not a propose, the result is undefined.
func proposeParts(m Msg) (val string) {
	val = string(m.Body()[proposeLen:])
	return
}

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

// Typically used after reading from the network, when building a new `Msg`
// object.
//
// This assumes the number of nodes fits in a byte.
func (m Msg) SetFrom(from int) {
	m[mFrom] = byte(from)
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
	case invite:
		return len(m.Body()) == inviteLen
	case rsvp:
		return len(m.Body()) >= rsvpLen
	case nominate:
		return len(m.Body()) >= nominateLen
	case vote:
		return len(m.Body()) >= voteLen
	}
	return false
}

func (m Msg) WireBytes() []byte {
	return m[mCmd:]
}

func (m *Msg) readFrom(c ReadFromer) (addr string, err os.Error) {
	n, a, er := c.ReadFrom(m.WireBytes())
	if er != nil {
		return "", er
	}
	*m = (*m)[0 : n+1] // truncate to fit
	return a.String(), nil
}

func ReadMsg(c ReadFromer, bound int) (m Msg, addr string, err os.Error) {
	m = make(Msg, bound)
	addr, err = m.readFrom(c)
	return
}

func (m Msg) HasFlags(flags int) bool {
	return m[mFlags]&byte(flags) != 0
}

func (m Msg) SetFlags(flags int) Msg {
	m[mFlags] |= byte(flags)
	return m
}

func (m Msg) ClearFlags(flags int) Msg {
	m[mFlags] &= ^byte(flags)
	return m
}

func (m *Msg) Dup() Msg {
	o := make(Msg, len(*m))
	copy(o, *m)
	return o
}
