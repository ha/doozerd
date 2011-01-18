package paxos

import (
	pb "goprotobuf.googlecode.com/hg/proto"
)

var (
	nop      = NewM_Cmd(M_NOP)
	invite   = NewM_Cmd(M_INVITE)
	rsvp     = NewM_Cmd(M_RSVP)
	nominate = NewM_Cmd(M_NOMINATE)
	vote     = NewM_Cmd(M_VOTE)
	tick     = NewM_Cmd(M_TICK)
	propose  = NewM_Cmd(M_PROPOSE)
	learn    = NewM_Cmd(M_LEARN)
)

var (
	msgTick = &M{WireCmd: tick}
)

func (m *M) From() int {
	return int(pb.GetInt32(m.WireFrom))
}

func (m *M) Cmd() int {
	return int(pb.GetInt32((*int32)(m.WireCmd)))
}

func (m *M) Seqn() int64 {
	return pb.GetInt64(m.WireSeqn)
}

// Typically used after reading from the network, when building a new `*M`
// object.
//
// This assumes the number of nodes fits in a byte.
func (m *M) SetFrom(from int32) {
	m.WireFrom = &from
}

// Typically used just before writing `m` to the network.
func (m *M) SetSeqn(seqn int64) {
	m.WireSeqn = &seqn
}

// Check that `m` is well-formed. Does not guarantee that it will be valid or
// meaningful. If this method returns `true`, you can safely pass `m` into a
// `Putter`.
func (m *M) Ok() bool {
	if m.WireCmd == nil {
		return false
	}

	if m.WireSeqn == nil {
		return false
	}

	switch m.Cmd() {
	case M_INVITE:
		return m.Crnd != nil
	case M_RSVP:
		return m.Crnd != nil && m.Vrnd != nil
	case M_NOMINATE:
		return m.Crnd != nil
	case M_VOTE:
		return m.Vrnd != nil
	case M_LEARN:
		return true
	}
	return false
}

func (m *M) Dup() *M {
	var n M
	n = *m
	n.Value = append([]byte{}, m.Value...)
	return &n
}
