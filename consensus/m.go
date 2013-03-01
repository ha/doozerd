package consensus

var (
	nop      = msg_NOP.Enum()
	invite   = msg_INVITE.Enum()
	rsvp     = msg_RSVP.Enum()
	nominate = msg_NOMINATE.Enum()
	vote     = msg_VOTE.Enum()
	tick     = msg_TICK.Enum()
	propose  = msg_PROPOSE.Enum()
	learn    = msg_LEARN.Enum()
)

const nmsg = 8

var (
	msgTick = &msg{Cmd: tick}
)
