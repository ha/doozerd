package consensus

import (
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


func TestHandleMInviteStopPartway(t *testing.T) {
	c := make(chan<- Packet, 100)
	cals := []string{"a"}
	addr := []string{"0"}
	var m Manager
	m.base = 2
	m.next = 5
	m.mrnd = make([]int64, 1)
	m.mult.cfg = 4
	m.run = map[int64]*run{
		2: &run{seqn: 2, cfg: 1, cals: cals, addr: addr, out: c},
		3: &run{seqn: 3, cfg: 1, cals: cals, addr: addr, out: c},
		4: &run{seqn: 4, cfg: 4, cals: cals, addr: addr, out: c},
	}
	m.handleMInvite(packet{
		Addr: "0",
		msg: msg{
			Seqn: proto.Int64(3),
			Cmd:  newMsg_Cmd(msg_MINVITE),
			Crnd: proto.Int64(2),
		},
	})
	assert.Equal(t, m.run[3].iAddr("0"), m.run[3].iLeader())
	assert.Equal(t, int64(2), m.run[3].a.rnd)
	assert.Equal(t, int64(0), m.run[4].a.rnd)
	assert.Equal(t, []int64{0}, m.mrnd)
}


func TestHandleMInviteToTheEnd(t *testing.T) {
	c := make(chan<- Packet, 100)
	cals := []string{"a"}
	addr := []string{"0"}
	var m Manager
	m.base = 2
	m.next = 5
	m.mrnd = make([]int64, 1)
	m.mult.cfg = 1
	m.cfg.Out = c
	m.run = map[int64]*run{
		2: &run{seqn: 2, cfg: 1, cals: cals, addr: addr, out: c},
		3: &run{seqn: 3, cfg: 1, cals: cals, addr: addr, out: c},
		4: &run{seqn: 4, cfg: 1, cals: cals, addr: addr, out: c},
	}
	m.handleMInvite(packet{
		Addr: "0",
		msg: msg{
			Seqn: proto.Int64(3),
			Cmd:  newMsg_Cmd(msg_MINVITE),
			Crnd: proto.Int64(2),
		},
	})
	assert.Equal(t, m.run[3].iAddr("0"), m.run[3].iLeader())
	assert.Equal(t, int64(2), m.run[3].a.rnd)
	assert.Equal(t, int64(2), m.run[4].a.rnd)
	assert.Equal(t, []int64{2}, m.mrnd)
}


func TestHandleMRsvp(t *testing.T) {
	var m Manager
	m.mult.cfg = 1
	m.mult.c.rsvps = make(map[string]bool)
	m.mult.c.quor = 2
	m.mult.seqn = 3
	m.handleMRsvp(packet{
		Addr: "0",
		msg: msg{
			Seqn: proto.Int64(3),
			Cmd:  newMsg_Cmd(msg_MINVITE),
			Crnd: proto.Int64(2),
		},
	})
	assert.Equal(t, false, m.mdone)
	m.handleMRsvp(packet{
		Addr: "1",
		msg: msg{
			Seqn: proto.Int64(3),
			Cmd:  newMsg_Cmd(msg_MINVITE),
			Crnd: proto.Int64(2),
		},
	})
	assert.Equal(t, true, m.mdone)
}
