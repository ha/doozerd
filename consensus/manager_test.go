package consensus

import (
	"code.google.com/p/goprotobuf/proto"
	"container/heap"
	"github.com/ha/doozerd/store"
	"github.com/bmizerany/assert"
	"net"
	"sort"
	"testing"
	"time"
)

// The first element in a protobuf stream is always a varint.
// The high bit of a varint byte indicates continuation;
// This is a continuation bit without a subsequent byte.
// http://code.google.com/apis/protocolbuffers/docs/encoding.html#varints.
var invalidProtobuf = []byte{0x80}

func mustMarshal(p interface{}) []byte {
	buf, err := proto.Marshal(p)
	if err != nil {
		panic(err)
	}
	return buf
}

func mustWait(s *store.Store, n int64) <-chan store.Event {
	c, err := s.Wait(store.Any, n)
	if err != nil {
		panic(err)
	}
	return c
}

func TestManagerPumpDropsOldPackets(t *testing.T) {
	st := store.New("")
	defer close(st.Ops)
	x := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}
	st.Ops <- store.Op{1, store.MustEncodeSet(node+"/a/addr", "1.2.3.4:5", 0), false}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/0", "a", 0), false}

	var m Manager
	m.run = make(map[int64]*run)
	m.event(<-mustWait(st, 2))
	m.pump()
	recvPacket(&m.packet, Packet{x, mustMarshal(&msg{Seqn: proto.Int64(1)})})
	m.pump()
	assert.Equal(t, 0, m.Stats.WaitPackets)
}

func TestRecvPacket(t *testing.T) {
	q := new(packets)
	x := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}

	p := recvPacket(q, Packet{x, mustMarshal(&msg{
		Seqn: proto.Int64(1),
		Cmd:  invite,
	})})
	assert.Equal(t, &packet{x, msg{Seqn: proto.Int64(1), Cmd: invite}}, p)
	p = recvPacket(q, Packet{x, mustMarshal(&msg{
		Seqn: proto.Int64(2),
		Cmd:  invite,
	})})
	assert.Equal(t, &packet{x, msg{Seqn: proto.Int64(2), Cmd: invite}}, p)
	p = recvPacket(q, Packet{x, mustMarshal(&msg{
		Seqn: proto.Int64(3),
		Cmd:  invite,
	})})
	assert.Equal(t, &packet{x, msg{Seqn: proto.Int64(3), Cmd: invite}}, p)
	assert.Equal(t, 3, q.Len())
}

func TestRecvEmptyPacket(t *testing.T) {
	q := new(packets)
	x := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}

	p := recvPacket(q, Packet{x, []byte{}})
	assert.Equal(t, (*packet)(nil), p)
	assert.Equal(t, 0, q.Len())
}

func TestRecvInvalidPacket(t *testing.T) {
	q := new(packets)
	x := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}
	p := recvPacket(q, Packet{x, invalidProtobuf})
	assert.Equal(t, (*packet)(nil), p)
	assert.Equal(t, 0, q.Len())
}

func TestSchedTrigger(t *testing.T) {
	var q triggers
	d := int64(15e8)

	t0 := time.Now().UnixNano()
	ts := t0 + d
	schedTrigger(&q, 1, t0, d)

	assert.Equal(t, 1, q.Len())
	f := q[0]
	assert.Equal(t, int64(1), f.n)
	assert.T(t, f.t == ts)
}

func TestManagerPacketProcessing(t *testing.T) {
	st := store.New("")
	defer close(st.Ops)
	in := make(chan Packet)
	out := make(chan Packet, 100)
	var m Manager
	m.run = make(map[int64]*run)
	m.Alpha = 1
	m.Store = st
	m.In = in
	m.Out = out
	m.Ops = st.Ops

	st.Ops <- store.Op{1, store.MustEncodeSet(node+"/a/addr", "1.2.3.4:5", 0), false}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/0", "a", 0), false}
	m.event(<-mustWait(st, 2))

	recvPacket(&m.packet, Packet{
		Data: mustMarshal(&msg{Seqn: proto.Int64(2), Cmd: learn, Value: []byte("foo")}),
		Addr: &net.UDPAddr{net.IP{127, 0, 0, 1}, 9999},
	})
	m.pump()
	assert.Equal(t, 0, m.packet.Len())
}

func TestManagerTickQueue(t *testing.T) {
	st := store.New("")
	defer close(st.Ops)
	st.Ops <- store.Op{1, store.MustEncodeSet(node+"/a/addr", "1.2.3.4:5", 0), false}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/0", "a", 0), false}

	var m Manager
	m.run = make(map[int64]*run)
	m.Alpha = 1
	m.Store = st
	m.Out = make(chan Packet, 100)
	m.event(<-mustWait(st, 2))

	// get it to tick for seqn 3
	recvPacket(&m.packet, Packet{Data: mustMarshal(&msg{Seqn: proto.Int64(3), Cmd: propose})})
	m.pump()
	assert.Equal(t, 1, m.tick.Len())

	m.doTick(time.Now().UnixNano() + initialWaitBound*2)
	assert.Equal(t, int64(1), m.Stats.TotalTicks)
}

func TestManagerFilterPropSeqn(t *testing.T) {
	ps := make(chan int64, 100)
	st := store.New("")
	defer close(st.Ops)

	m := &Manager{
		DefRev: 2,
		Alpha:  1,
		Self:   "b",
		PSeqn:  ps,
		Store:  st,
	}
	go m.Run()

	st.Ops <- store.Op{1, store.MustEncodeSet("/ctl/cal/0", "a", 0), false}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/1", "b", 0), false}
	st.Ops <- store.Op{3, store.Nop, false}
	st.Ops <- store.Op{4, store.Nop, false}
	assert.Equal(t, int64(3), <-ps)
	assert.Equal(t, int64(5), <-ps)

	st.Ops <- store.Op{5, store.Nop, false}
	st.Ops <- store.Op{6, store.Nop, false}
	assert.Equal(t, int64(7), <-ps)
}

func TestManagerProposalQueue(t *testing.T) {
	var m Manager
	m.run = make(map[int64]*run)
	m.propose(&m.packet, &Prop{Seqn: 1, Mut: []byte("foo")}, time.Now().UnixNano())
	assert.Equal(t, 1, m.packet.Len())
}

func TestManagerProposeFill(t *testing.T) {
	q := new(packets)
	var m Manager
	m.Self = "a"
	m.run = map[int64]*run{
		6: &run{seqn: 6, cals: []string{"a", "b", "c"}},
		7: &run{seqn: 7, cals: []string{"a", "b", "c"}},
		8: &run{seqn: 8, cals: []string{"a", "b", "c"}},
	}
	exp := triggers{
		{123, 7},
		{123, 8},
	}
	m.propose(q, &Prop{Seqn: 9, Mut: []byte("foo")}, 123)
	assert.Equal(t, exp, m.fill)
}

func TestApplyTriggers(t *testing.T) {
	pkts := new(packets)
	tgrs := new(triggers)

	heap.Push(tgrs, trigger{t: 1, n: 1})
	heap.Push(tgrs, trigger{t: 2, n: 2})
	heap.Push(tgrs, trigger{t: 3, n: 3})
	heap.Push(tgrs, trigger{t: 4, n: 4})
	heap.Push(tgrs, trigger{t: 5, n: 5})
	heap.Push(tgrs, trigger{t: 6, n: 6})
	heap.Push(tgrs, trigger{t: 7, n: 7})
	heap.Push(tgrs, trigger{t: 8, n: 8})
	heap.Push(tgrs, trigger{t: 9, n: 9})

	n := applyTriggers(pkts, tgrs, 5, &msg{Cmd: tick})
	assert.Equal(t, 5, n)

	expTriggers := new(triggers)
	expPackets := new(packets)
	heap.Push(expPackets, &packet{msg: msg{Cmd: tick, Seqn: proto.Int64(1)}})
	heap.Push(expPackets, &packet{msg: msg{Cmd: tick, Seqn: proto.Int64(2)}})
	heap.Push(expPackets, &packet{msg: msg{Cmd: tick, Seqn: proto.Int64(3)}})
	heap.Push(expPackets, &packet{msg: msg{Cmd: tick, Seqn: proto.Int64(4)}})
	heap.Push(expPackets, &packet{msg: msg{Cmd: tick, Seqn: proto.Int64(5)}})
	heap.Push(expTriggers, trigger{t: 6, n: 6})
	heap.Push(expTriggers, trigger{t: 7, n: 7})
	heap.Push(expTriggers, trigger{t: 8, n: 8})
	heap.Push(expTriggers, trigger{t: 9, n: 9})

	sort.Sort(pkts)
	sort.Sort(tgrs)
	sort.Sort(expPackets)
	sort.Sort(expTriggers)

	assert.Equal(t, expTriggers, tgrs)
	assert.Equal(t, expPackets, pkts)
}

func TestManagerEvent(t *testing.T) {
	const alpha = 2
	runs := make(map[int64]*run)
	st := store.New("")
	defer close(st.Ops)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(node+"/a/addr", "1.2.3.4:5", 0),
	}

	st.Ops <- store.Op{
		Seqn: 2,
		Mut:  store.MustEncodeSet(cal+"/1", "a", 0),
	}

	ch, err := st.Wait(store.Any, 2)
	if err != nil {
		panic(err)
	}

	x, _ := net.ResolveUDPAddr("udp", "1.2.3.4:5")
	pseqn := make(chan int64, 1)
	m := &Manager{
		Alpha: alpha,
		Self:  "a",
		PSeqn: pseqn,
		Ops:   st.Ops,
		Out:   make(chan Packet),
		run:   runs,
	}
	m.event(<-ch)

	exp := &run{
		self:  "a",
		seqn:  2 + alpha,
		cals:  []string{"a"},
		addr:  []*net.UDPAddr{x},
		ops:   st.Ops,
		out:   m.Out,
		bound: initialWaitBound,
	}
	exp.c = coordinator{
		crnd: 1,
		size: 1,
		quor: exp.quorum(),
	}
	exp.l = learner{
		round:  1,
		size:   1,
		quorum: int64(exp.quorum()),
		votes:  map[string]int64{},
		voted:  []bool{false},
	}

	assert.Equal(t, 1, len(runs))
	assert.Equal(t, exp, runs[exp.seqn])
	assert.Equal(t, exp.seqn, <-pseqn)
	assert.Equal(t, exp.seqn+1, m.next)
}

func TestManagerRemoveLastCal(t *testing.T) {
	const alpha = 2
	runs := make(map[int64]*run)
	st := store.New("")
	defer close(st.Ops)

	st.Ops <- store.Op{1, store.MustEncodeSet(node+"/a/addr", "1.2.3.4:5", 0), false}
	st.Ops <- store.Op{2, store.MustEncodeSet(cal+"/1", "a", 0), false}
	st.Ops <- store.Op{3, store.MustEncodeSet(cal+"/1", "", -1), false}

	x, _ := net.ResolveUDPAddr("udp", "1.2.3.4:5")
	pseqn := make(chan int64, 100)
	m := &Manager{
		Alpha: alpha,
		Self:  "a",
		PSeqn: pseqn,
		Ops:   st.Ops,
		Out:   make(chan Packet),
		run:   runs,
	}
	m.event(<-mustWait(st, 2))
	m.event(<-mustWait(st, 3))

	exp := &run{
		self:  "a",
		seqn:  3 + alpha,
		cals:  []string{"a"},
		addr:  []*net.UDPAddr{x},
		ops:   st.Ops,
		out:   m.Out,
		bound: initialWaitBound,
	}
	exp.c = coordinator{
		crnd: 1,
		size: 1,
		quor: exp.quorum(),
	}
	exp.l = learner{
		round:  1,
		size:   1,
		quorum: int64(exp.quorum()),
		votes:  map[string]int64{},
		voted:  []bool{false},
	}

	assert.Equal(t, 2, len(runs))
	assert.Equal(t, exp, runs[exp.seqn])
	assert.Equal(t, exp.seqn+1, m.next)
}

func TestDelRun(t *testing.T) {
	const alpha = 2
	runs := make(map[int64]*run)
	st := store.New("")
	defer close(st.Ops)

	st.Ops <- store.Op{1, store.MustEncodeSet(node+"/a/addr", "x", 0), false}
	st.Ops <- store.Op{2, store.MustEncodeSet(cal+"/1", "a", 0), false}
	st.Ops <- store.Op{3, store.Nop, false}
	st.Ops <- store.Op{4, store.Nop, false}

	c2, err := st.Wait(store.Any, 2)
	if err != nil {
		panic(err)
	}

	c3, err := st.Wait(store.Any, 3)
	if err != nil {
		panic(err)
	}

	c4, err := st.Wait(store.Any, 4)
	if err != nil {
		panic(err)
	}

	pseqn := make(chan int64, 100)
	m := &Manager{
		Alpha: alpha,
		Self:  "a",
		PSeqn: pseqn,
		Ops:   st.Ops,
		Out:   make(chan Packet),
		run:   runs,
	}
	m.event(<-c2)
	assert.Equal(t, 1, len(m.run))
	m.event(<-c3)
	assert.Equal(t, 2, len(m.run))
	m.event(<-c4)
	assert.Equal(t, 2, len(m.run))
}

func TestGetCalsFull(t *testing.T) {
	st := store.New("")
	defer close(st.Ops)

	st.Ops <- store.Op{Seqn: 1, Mut: store.MustEncodeSet(cal+"/1", "a", 0)}
	st.Ops <- store.Op{Seqn: 2, Mut: store.MustEncodeSet(cal+"/2", "c", 0)}
	st.Ops <- store.Op{Seqn: 3, Mut: store.MustEncodeSet(cal+"/3", "b", 0)}
	<-st.Seqns

	assert.Equal(t, []string{"a", "b", "c"}, getCals(st))
}

func TestGetCalsPartial(t *testing.T) {
	st := store.New("")
	defer close(st.Ops)

	st.Ops <- store.Op{Seqn: 1, Mut: store.MustEncodeSet(cal+"/1", "a", 0)}
	st.Ops <- store.Op{Seqn: 2, Mut: store.MustEncodeSet(cal+"/2", "", 0)}
	st.Ops <- store.Op{Seqn: 3, Mut: store.MustEncodeSet(cal+"/3", "", 0)}
	<-st.Seqns

	assert.Equal(t, []string{"a"}, getCals(st))
}

func TestGetAddrs(t *testing.T) {
	st := store.New("")
	defer close(st.Ops)

	st.Ops <- store.Op{1, store.MustEncodeSet(node+"/1/addr", "1.2.3.4:5", 0), false}
	st.Ops <- store.Op{2, store.MustEncodeSet(node+"/2/addr", "2.3.4.5:6", 0), false}
	st.Ops <- store.Op{3, store.MustEncodeSet(node+"/3/addr", "3.4.5.6:7", 0), false}
	<-st.Seqns

	x, _ := net.ResolveUDPAddr("udp", "1.2.3.4:5")
	y, _ := net.ResolveUDPAddr("udp", "2.3.4.5:6")
	z, _ := net.ResolveUDPAddr("udp", "3.4.5.6:7")
	addrs := getAddrs(st, []string{"1", "2", "3"})
	assert.Equal(t, []*net.UDPAddr{x, y, z}, addrs)
}
