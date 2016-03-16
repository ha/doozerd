package consensus

import (
	"github.com/bmizerany/assert"
	"github.com/golang/protobuf/proto"
	"github.com/ha/doozerd/store"
	"net"
	"testing"
)

const (
	node = "/ctl/node"
	cal  = "/ctl/cal"
)

func MustResolveUDPAddr(n, addr string) *net.UDPAddr {
	udp, err := net.ResolveUDPAddr(n, addr)
	if err != nil {
		panic(err)
	}

	return udp
}

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
	r.l.init(1, 1)

	p := packet{
		msg: msg{
			Seqn:  proto.Int64(1),
			Cmd:   vote,
			Vrnd:  proto.Int64(1),
			Value: []byte("foo"),
		},
		Addr: MustResolveUDPAddr("udp", "1.2.3.4:5"),
	}

	r.update(&p, 0, new(triggers))

	assert.Equal(t, true, r.l.done)
	assert.Equal(t, "foo", r.l.v)
}

func TestRunInviteDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.update(&packet{msg: *newInviteSeqn1(1)}, 0, new(triggers))

	assert.Equal(t, int64(1), r.a.rnd)
}

func TestRunProposeDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.update(&packet{msg: msg{Cmd: propose}}, -1, new(triggers))
	assert.Equal(t, true, r.c.begun)
}

func TestRunSendsCoordPacket(t *testing.T) {
	c := make(chan Packet, 100)
	x := MustResolveUDPAddr("udp", "1.2.3.4:5")
	y := MustResolveUDPAddr("udp", "2.3.4.5:6")
	var r run
	r.c.crnd = 1
	r.out = c
	r.addr = []*net.UDPAddr{x, y}

	var got msg
	exp := msg{
		Seqn: proto.Int64(0),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	r.update(&packet{msg: *newPropose("foo")}, -1, new(triggers))
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
	ticks := new(triggers)

	r.update(&packet{msg: *newPropose("foo")}, -1, ticks)

	assert.Equal(t, 1, ticks.Len())
}

func TestRunSendsAcceptorPacket(t *testing.T) {
	c := make(chan Packet, 100)
	x := MustResolveUDPAddr("udp", "1.2.3.4:5")
	y := MustResolveUDPAddr("udp", "2.3.4.5:6")
	var r run
	r.out = c
	r.addr = []*net.UDPAddr{x, y}

	var got msg
	exp := msg{
		Seqn:  proto.Int64(0),
		Cmd:   rsvp,
		Crnd:  proto.Int64(1),
		Vrnd:  proto.Int64(0),
		Value: []byte{},
	}

	r.update(&packet{msg: *newInviteSeqn1(1)}, 0, new(triggers))
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
	r.addr = []*net.UDPAddr{nil, nil}
	r.l.init(1, 1)

	var got msg
	exp := msg{
		Seqn:  proto.Int64(0),
		Cmd:   learn,
		Value: []byte("foo"),
	}

	r.update(&packet{msg: *newVote(1, "foo")}, 0, new(triggers))
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
	r.l.init(1, 1)

	r.update(&packet{msg: *newVote(1, "foo")}, 0, new(triggers))
	assert.Equal(t, store.Op{1, "foo"}, <-c)
}

func TestRunBroadcastThree(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.seqn = 1
	r.out = c
	r.addr = []*net.UDPAddr{
		MustResolveUDPAddr("udp", "1.2.3.4:5"),
		MustResolveUDPAddr("udp", "2.3.4.5:6"),
		MustResolveUDPAddr("udp", "3.4.5.6:7"),
	}

	r.broadcast(newInvite(1))
	c <- Packet{}

	exp := msg{
		Seqn: proto.Int64(1),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	addr := make([]*net.UDPAddr, len(r.addr))
	for i := 0; i < len(r.addr); i++ {
		p := <-c
		addr[i] = p.Addr
		var got msg
		err := proto.Unmarshal(p.Data, &got)
		assert.Equal(t, nil, err)
		assert.Equal(t, exp, got)
	}

	assert.Equal(t, Packet{}, <-c)
	assert.Equal(t, r.addr, addr)
}

func TestRunBroadcastFive(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.seqn = 1
	r.out = c
	r.addr = []*net.UDPAddr{
		MustResolveUDPAddr("udp", "1.2.3.4:5"),
		MustResolveUDPAddr("udp", "2.3.4.5:6"),
		MustResolveUDPAddr("udp", "3.4.5.6:7"),
		MustResolveUDPAddr("udp", "4.5.6.7:8"),
		MustResolveUDPAddr("udp", "5.6.7.8:9"),
	}

	r.broadcast(newInvite(1))
	c <- Packet{}

	exp := msg{
		Seqn: proto.Int64(1),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	addr := make([]*net.UDPAddr, len(r.addr))
	for i := 0; i < len(r.addr); i++ {
		p := <-c
		addr[i] = p.Addr
		var got msg
		err := proto.Unmarshal(p.Data, &got)
		assert.Equal(t, nil, err)
		assert.Equal(t, exp, got)
	}

	assert.Equal(t, Packet{}, <-c)
	assert.Equal(t, r.addr, addr)
}

func TestRunBroadcastNil(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.out = c
	r.addr = []*net.UDPAddr{
		MustResolveUDPAddr("udp", "1.2.3.4:5"),
		MustResolveUDPAddr("udp", "2.3.4.5:6"),
		MustResolveUDPAddr("udp", "3.4.5.6:7"),
	}

	r.broadcast(nil)
	c <- Packet{}
	assert.Equal(t, Packet{}, <-c)
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

func TestRunReturnTrueIfLearned(t *testing.T) {
	r := run{}
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	p := packet{msg: msg{
		Seqn:  proto.Int64(1),
		Cmd:   learn,
		Value: []byte("foo"),
	}}

	r.update(&p, 0, new(triggers))
	assert.T(t, r.l.done)
}

func TestRunReturnFalseIfNotLearned(t *testing.T) {
	r := run{}
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	p := packet{msg: msg{
		Seqn:  proto.Int64(1),
		Cmd:   invite,
		Value: []byte("foo"),
	}}

	r.update(&p, 0, new(triggers))
	assert.T(t, !r.l.done)
}

func TestRunIndexOfNilAddr(t *testing.T) {
	r := run{addr: []*net.UDPAddr{new(net.UDPAddr)}}
	assert.Equal(t, -1, r.indexOfAddr(nil))
}
