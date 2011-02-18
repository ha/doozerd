package consensus

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
	msgTick = &M{Cmd: tick}
)

// Typically used just before writing `m` to the network.
func (m *M) SetSeqn(seqn int64) {
	m.Seqn = &seqn
}
