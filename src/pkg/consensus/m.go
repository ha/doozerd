package consensus

var (
	nop      = newMsg_Cmd(msg_NOP)
	invite   = newMsg_Cmd(msg_INVITE)
	rsvp     = newMsg_Cmd(msg_RSVP)
	nominate = newMsg_Cmd(msg_NOMINATE)
	vote     = newMsg_Cmd(msg_VOTE)
	tick     = newMsg_Cmd(msg_TICK)
	propose  = newMsg_Cmd(msg_PROPOSE)
	learn    = newMsg_Cmd(msg_LEARN)
)

var (
	msgTick = &msg{Cmd: tick}
)
