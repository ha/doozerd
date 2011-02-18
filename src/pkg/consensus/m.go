package consensus

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
