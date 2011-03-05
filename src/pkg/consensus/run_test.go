package consensus

import (
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


const (
	info = "/doozer/info"
	slot = "/doozer/slot"
)


type msgSlot struct {
	*M
}


func (ms msgSlot) Put(m *M) {
	*ms.M = *m
}


func TestGetCalsFull(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{Seqn: 1, Mut: store.MustEncodeSet(slot+"/1", "a", 0)}
	st.Ops <- store.Op{Seqn: 2, Mut: store.MustEncodeSet(slot+"/2", "c", 0)}
	st.Ops <- store.Op{Seqn: 3, Mut: store.MustEncodeSet(slot+"/3", "b", 0)}
	<-st.Seqns

	assert.Equal(t, []string{"a", "b", "c"}, getCals(st))
}


func TestGetCalsPartial(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{Seqn: 1, Mut: store.MustEncodeSet(slot+"/1", "a", 0)}
	st.Ops <- store.Op{Seqn: 2, Mut: store.MustEncodeSet(slot+"/2", "", 0)}
	st.Ops <- store.Op{Seqn: 3, Mut: store.MustEncodeSet(slot+"/3", "", 0)}
	<-st.Seqns

	assert.Equal(t, []string{"a"}, getCals(st))
}


func TestGetAddrs(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{1, store.MustEncodeSet(info+"/1/addr", "x", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet(info+"/2/addr", "y", 0)}
	st.Ops <- store.Op{3, store.MustEncodeSet(info+"/3/addr", "z", 0)}
	<-st.Seqns

	assert.Equal(t, map[string]bool{"x": true, "y": true, "z": true}, getAddrs(st))
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


func alphaTest(t *testing.T, alpha int64) {
	runs := make(chan *run)
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(info+"/a/addr", "x", 0),
	}

	st.Ops <- store.Op{
		Seqn: 2,
		Mut:  store.MustEncodeSet(slot+"/1", "a", 0),
	}

	for 2 != <-st.Seqns {
	}

	tr := run{
		self:  "a",
		ops:   st.Ops,
		out:   make(chan Packet),
		bound: initialWaitBound,
	}
	go generateRuns(alpha, st.Watch(store.Any), runs, tr)

	// The only way to generate a run is on an event.
	// Send a noop here to get things started.
	st.Ops <- store.Op{3, store.Nop}

	exp := &run{
		self:  "a",
		seqn:  3 + alpha,
		cals:  []string{"a"},
		addrs: map[string]bool{"x": true},
		ops:   st.Ops,
		out:   tr.out,
		bound: initialWaitBound,
	}
	exp.c = coordinator{
		crnd: 1,
		size: 1,
		quor: exp.quorum(),
	}
	exp.l = learner{
		round:  1,
		quorum: int64(exp.quorum()),
		votes:  map[string]int64{},
		voted:  map[string]bool{},
	}

	assert.Equal(t, exp, <-runs)
}


func TestRunAlphaOfOne(t *testing.T) {
	alphaTest(t, 1)
}


func TestRunAlphaOfThree(t *testing.T) {
	alphaTest(t, 3)
}


func TestRunAlphaOfFifty(t *testing.T) {
	alphaTest(t, 50)
}


func TestRunAfterWatch(t *testing.T) {
	alpha := int64(3)
	runs := make(chan *run)
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(info+"/b/addr", "y", 0),
	}

	for 1 != <-st.Seqns {
	}

	tr := run{
		self:  "b",
		ops:   st.Ops,
		out:   make(chan Packet),
		bound: initialWaitBound,
	}
	go generateRuns(alpha, st.Watch(store.Any), runs, tr)

	st.Ops <- store.Op{
		Seqn: 2,
		Mut:  store.MustEncodeSet(slot+"/1", "b", 0),
	}

	exp := &run{
		self:  "b",
		seqn:  2 + alpha,
		cals:  []string{"b"},
		addrs: map[string]bool{"y": true},
		ops:   st.Ops,
		out:   tr.out,
		bound: initialWaitBound,
	}
	exp.c = coordinator{
		crnd: 1,
		size: 1,
		quor: exp.quorum(),
	}
	exp.l = learner{
		round:  1,
		quorum: int64(exp.quorum()),
		votes:  map[string]int64{},
		voted:  map[string]bool{},
	}

	assert.Equal(t, exp, <-runs)
}


func TestRunVoteDeliverd(t *testing.T) {
	r := run{}
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)
	r.l.init(1)

	p := packet{
		M: M{
			Seqn:  proto.Int64(1),
			Cmd:   vote,
			Vrnd:  proto.Int64(1),
			Value: []byte("foo"),
		},
		Addr: "X",
	}

	r.deliver(p)

	assert.Equal(t, true, r.l.done)
	assert.Equal(t, "foo", r.l.v)
}


func TestRunInviteDeliverd(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.deliver(packet{M: *newInviteSeqn1(1)})

	assert.Equal(t, int64(1), r.a.rnd)
}


func TestRunProposeDeliverd(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)
	r.ticks = make(chan int64, 100)

	r.deliver(packet{M: M{Cmd: propose}})
	assert.Equal(t, true, r.c.begun)
}


func TestRunSendsCoordPacket(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.c.crnd = 1
	r.out = c
	r.ticks = make(chan int64, 100)
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
	}

	var got M
	exp := M{
		Seqn: proto.Int64(0),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	r.deliver(packet{M: *newPropose("foo")})
	assert.Equal(t, 2, len(c))
	err := proto.Unmarshal((<-c).Data, &got)
	assert.Equal(t, nil, err)
	assert.Equal(t, exp, got)
}


func TestRunSchedulesTick(t *testing.T) {
	ticks := make(chan int64)
	var r run
	r.seqn = 1
	r.out = make(chan Packet, 100)
	r.ticks = ticks

	r.deliver(packet{M: *newPropose("foo")})

	r.deliver(packet{M: *newRsvp(2, 0, "")})
	assert.Equal(t, int64(1), <-ticks)
}


func TestRunSendsAcceptorPacket(t *testing.T) {
	c := make(chan Packet, 100)
	var r run
	r.out = c
	r.addrs = map[string]bool{
		"x": true,
		"y": true,
	}

	var got M
	exp := M{
		Seqn: proto.Int64(0),
		Cmd:  rsvp,
		Crnd: proto.Int64(1),
		Vrnd: proto.Int64(0),
	}

	r.deliver(packet{M: *newInviteSeqn1(1)})
	assert.Equal(t, 2, len(c))
	err := proto.Unmarshal((<-c).Data, &got)
	assert.Equal(t, nil, err)
	assert.Equal(t, exp, got)
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

	var got M
	exp := M{
		Seqn:  proto.Int64(0),
		Cmd:   learn,
		Value: []byte("foo"),
	}

	r.deliver(packet{M: *newVote(1, "foo")})
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

	r.deliver(packet{M: *newVote(1, "foo")})
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

	exp := M{
		Seqn: proto.Int64(1),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	addrs := map[string]bool{}
	for i := 0; i < len(r.addrs); i++ {
		p := <-c
		addrs[p.Addr] = true
		var got M
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

	exp := M{
		Seqn: proto.Int64(1),
		Cmd:  invite,
		Crnd: proto.Int64(1),
	}

	addrs := map[string]bool{}
	for i := 0; i < len(r.addrs); i++ {
		p := <-c
		addrs[p.Addr] = true
		var got M
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
		M: M{
			Seqn:  proto.Int64(1),
			Cmd:   vote,
			Vrnd:  proto.Int64(1),
			Value: []byte("foo"),
		},
		Addr: "X",
	}

	r.deliver(p)

	assert.Equal(t, exp, r.l)
}


func TestRunInviteDoneAndNotDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)

	r.l.done = true
	exp := r.a

	r.deliver(packet{M: *newInviteSeqn1(1)})

	assert.Equal(t, exp, r.a)
}


func TestRunProposeDoneAndNotDelivered(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.ops = make(chan store.Op, 100)
	r.l.done = true

	exp := r.c

	r.deliver(packet{M: M{Cmd: propose}})
	assert.Equal(t, exp, r.c)
}


func TestRunRepliesWithLearnIfAlreadyDone(t *testing.T) {
	var r run
	out := make(chan Packet, 100)
	r.out = out
	r.ops = make(chan store.Op, 100)
	r.l.v = "foobar"
	r.l.done = true

	r.deliver(packet{M: M{Cmd: invite}, Addr: "123"})

	m:= M{Cmd: learn, Value: []byte("foobar"), Seqn: &r.seqn}
	buf, err := proto.Marshal(&m)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, Packet{"123", buf}, <-out)
}


func TestRunSendLearn(t *testing.T) {
	var r run
	r.out = make(chan Packet, 100)
	r.l.done = true

	r.deliver(packet{M: M{Cmd: rsvp}, Addr: "123"})
	r.deliver(packet{M: M{Cmd: nominate}, Addr: "123"})
	r.deliver(packet{M: M{Cmd: vote}, Addr: "123"})
	r.deliver(packet{M: M{Cmd: nop}, Addr: "123"})
	r.deliver(packet{M: M{Cmd: tick}, Addr: "123"})
	r.deliver(packet{M: M{Cmd: learn}, Addr: "123"})
	r.deliver(packet{M: M{Cmd: propose}, Addr: "123"})

	assert.Equal(t, 0, len(r.out))
}
