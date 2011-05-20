package consensus

import "strconv"

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


func (m msg) String() (s string) {
	if m.Seqn == nil {
		return "msg{}"
	}
	s = "msg{" + strconv.Itoa64(*m.Seqn)
	if m.Cmd != nil {
		s += " " + msg_Cmd_name[int32(*m.Cmd)]
	}
	if m.Crnd != nil {
		s += ", Crnd:" + strconv.Itoa64(*m.Crnd)
	}
	if m.Vrnd != nil {
		s += ", Vrnd:" + strconv.Itoa64(*m.Vrnd)
	}
	if len(m.Value) > 0 {
		s += ", Value:" + string(m.Value)
	}
	return s + "}"
}
