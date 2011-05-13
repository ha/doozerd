package consensus

import (
	"container/vector"
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


const (
	node = "/ctl/node"
	cal  = "/ctl/cal"
)


type msgSlot struct {
	*msg
}


func (ms msgSlot) Put(m *msg) {
	*ms.msg = *m
}


func TestQuorum(t *testing.T) {
	assert.Equal(t, 1, (&run{cals: []string{"a"}}).quorum())
	assert.Equal(t, 2, (&run{cals: []string{"a", "b"}}).quorum())
	assert.Equal(t, 2, (&run{cals: []string{"a", "b", "c"}}).quorum())
	assert.Equal(t, 3, (&run{cals: []string{"a", "b", "c", "d"}}).quorum())
	assert.Equal(t, 3, (&run{cals: []string{"a", "b", "c", "d", "e"}}).quorum())
	assert.Equal(t, 4, (&run{cals: []string{"a", "b", "c", "d", "e", "f"}}).quorum())
	assert.Equal(t, 4, (&run{cals: []string{"a", "b", "c", "d", "e", "f", "g"}}).quorum())
	assert.Equal(t, 5, (&run{cals: []string{"a", "b", "c", "d", "e", "f", "g", "h"}}).quorum())
}


func TestRunVoteDelivered(t *testing.T) {
	r := run{}
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)
	r.l.init(1)

	p := packet{
		msg: msg{
			Seqn:  proto.Int64(1),
			Cmd:   vote,
			Vrnd:  proto.Int64(1),
			Value: []byte("foo"),
		},
		Addr: "X",
	}

	r.update(p, new(vector.Vector))

	assert.Equal(t, true, r.l.done)
	assert.Equal(t, "foo", r.l.v)
}


func TestRunInviteDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.update(packet{msg: *newInviteSeqn1(1)}, new(vector.Vector))

	assert.Equal(t, int64(1), r.a.rnd)
}


func TestRunProposeDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.update(packet{msg: msg{Cmd: propose}}, new(vector.Vector))
	assert.Equal(t, true, r.c.begun)
}


func TestRunSendsCoordPacket(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.c.crnd = 1
	r.out = c
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
	}

	var got msg
	exp := msg{
		Seqn: proto.Int64(0),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	r.update(packet{msg: *newPropose("foo")}, new(vector.Vector))
	<-c
	err := proto.Unmarshal((<-c).Data, &got)
	assert.Equal(t, nil, err)
	assert.Equal(t, exp, got)
	assert.Equal(t, 0, len(c))
}


func TestRunSchedulesTick(t *testing.T) {
	var r run
	r.seqn = 1
	r.bound = 10
	r.out = make(chan Packet, 100)
	ticks := new(vector.Vector)

	r.update(packet{msg: *newPropose("foo")}, ticks)

	assert.Equal(t, 1, ticks.Len())
}


func TestRunSendsAcceptorPacket(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.out = c
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
	}

	var got msg
	exp := msg{
		Seqn: proto.Int64(0),
		Cmd:  rsvp,
		Crnd: proto.Int64(1),
		Vrnd: proto.Int64(0),
	}

	r.update(packet{msg: *newInviteSeqn1(1)}, new(vector.Vector))
	<-c
	err := proto.Unmarshal((<-c).Data, &got)
	assert.Equal(t, nil, err)
	assert.Equal(t, exp, got)
	assert.Equal(t, 0, len(c))
}


func TestRunSendsLearnerPacket(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.out = c
	r.ops = make(chan store.Op, 100)
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
	}

	var got msg
	exp := msg{
		Seqn:  proto.Int64(0),
		Cmd:   learn,
		Value: []byte("foo"),
	}

	r.update(packet{msg: *newVote(1, "foo")}, new(vector.Vector))
	assert.Equal(t, 2, len(c))
	err := proto.Unmarshal((<-c).Data, &got)
	assert.Equal(t, nil, err)
	assert.Equal(t, exp, got)
}


func TestRunAppliesOp(t *testing.T) {
	c := make(chan store.Op, 100)
	var r run
	r.seqn = 1
	r.out = make(chan Packet, 100)
	r.ops = c
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
	}

	r.update(packet{msg: *newVote(1, "foo")}, new(vector.Vector))
	assert.Equal(t, store.Op{1, "foo"}, <-c)
}


func TestRunBroadcastThree(t *testing.T) {
	c := make(chan Packet, 100)
	sentinel := Packet{Addr: "sentinel"}
	var r run
	r.seqn = 1
	r.out = c
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
		"z": true,
	}

	r.broadcast(newInvite(1))
	c <- sentinel

	exp := msg{
		Seqn: proto.Int64(1),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	addrs := map[string]bool{}
	for i := 0; i < len(r.addrs); i++ {
		p := <-c
		addrs[p.Addr] = true
		var got msg
		err := proto.Unmarshal(p.Data, &got)
		assert.Equal(t, nil, err)
		assert.Equal(t, exp, got)
	}

	assert.Equal(t, sentinel, <-c)
	assert.Equal(t, r.addrs, addrs)
}


func TestRunBroadcastFive(t *testing.T) {
	c := make(chan Packet, 100)
	sentinel := Packet{Addr: "sentinel"}
	var r run
	r.seqn = 1
	r.out = c
	r.addrs = map[string]bool{
		"v": true,
		"w": true,
		"x": true,
		"y": true,
		"z": true,
	}

	r.broadcast(newInvite(1))
	c <- sentinel

	exp := msg{
		Seqn: proto.Int64(1),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	addrs := map[string]bool{}
	for i := 0; i < len(r.addrs); i++ {
		p := <-c
		addrs[p.Addr] = true
		var got msg
		err := proto.Unmarshal(p.Data, &got)
		assert.Equal(t, nil, err)
		assert.Equal(t, exp, got)
	}

	assert.Equal(t, sentinel, <-c)
	assert.Equal(t, r.addrs, addrs)
}


func TestRunBroadcastNil(t *testing.T) {
	c := make(chan Packet, 100)
	sentinel := Packet{Addr: "sentinel"}
	var r run
	r.out = c
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
		"z": true,
	}

	r.broadcast(nil)
	c <- sentinel

	assert.Equal(t, sentinel, <-c)
}


func TestRunIsLeader(t *testing.T) {
	r := &run{
		cals: []string{"a", "b", "c"}, // len(cals) == 3
		seqn: 3,                       // 3 % 3 == 0
	}

	assert.T(t, r.isLeader("a"))  // index == 0
	assert.T(t, !r.isLeader("b")) // index == 1
	assert.T(t, !r.isLeader("c")) // index == 2
	assert.T(t, !r.isLeader("x")) // index DNE
}


func TestRunVoteDoneAndNotDelivered(t *testing.T) {
	r := run{}
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.l.init(1)
	r.l.done = true

	exp := r.l

	p := packet{
		msg: msg{
			Seqn:  proto.Int64(1),
			Cmd:   vote,
			Vrnd:  proto.Int64(1),
			Value: []byte("foo"),
		},
		Addr: "X",
	}

	r.update(p, new(vector.Vector))

	assert.Equal(t, exp, r.l)
}


func TestRunInviteDoneAndNotDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.l.done = true
	exp := r.a

	r.update(packet{msg: *newInviteSeqn1(1)}, new(vector.Vector))

	assert.Equal(t, exp, r.a)
}


func TestRunProposeDoneAndNotDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)
	r.l.done = true

	exp := r.c

	r.update(packet{msg: msg{Cmd: propose}}, new(vector.Vector))
	assert.Equal(t, exp, r.c)
}


func TestRunReturnTrueIfLearned(t *testing.T) {
	r := run{}
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	p := packet{
		msg: msg{
			Seqn:  proto.Int64(1),
			Cmd:   learn,
			Value: []byte("foo"),
		},
		Addr: "X",
	}

	learned := r.update(p, new(vector.Vector))
	assert.T(t, learned)
}


func TestRunReturnFalseIfNotLearned(t *testing.T) {
	r := run{}
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	p := packet{
		msg: msg{
			Seqn:  proto.Int64(1),
			Cmd:   invite,
			Value: []byte("foo"),
		},
		Addr: "X",
	}

	learned := r.update(p, new(vector.Vector))
	assert.T(t, !learned)
}
