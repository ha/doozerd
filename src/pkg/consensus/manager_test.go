package consensus


import (
	"container/heap"
	"container/vector"
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
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


func TestManagerPacketQueue(t *testing.T) {
	in := make(chan Packet)

	st := store.New()
	out := make(chan Packet, 100)
	cfg := &Config{
		Store: st,
		In:    in,
		Out:   out,
	}
	s := make(chan Stats)
	m := &Manager{Stats: s, cfg: *cfg}
	go m.manage(s)

	in <- Packet{"x", mustMarshal(&msg{
		Seqn: proto.Int64(1),
		Cmd:  invite,
	})}

	assert.Equal(t, 1, (<-m.Stats).WaitPackets)
}


func TestManagerDropsOldPackets(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	in := make(chan Packet)
	out := make(chan Packet, 100)
	cfg := &Config{
		Store: st,
		In:    in,
		Out:   out,
	}
	m := NewManager(cfg)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(node+"/a/addr", "x", 0),
	}
	for (<-m.Stats).Runs < 1 {
	}

	in <- Packet{"x", mustMarshal(&msg{Seqn: proto.Int64(1)})}

	assert.Equal(t, 0, (<-m.Stats).WaitPackets)
}


func TestRecvPacket(t *testing.T) {
	q := new(vector.Vector)

	recvPacket(q, Packet{"x", mustMarshal(&msg{
		Seqn: proto.Int64(1),
		Cmd:  invite,
	})})
	recvPacket(q, Packet{"x", mustMarshal(&msg{
		Seqn: proto.Int64(2),
		Cmd:  invite,
	})})
	recvPacket(q, Packet{"x", mustMarshal(&msg{
		Seqn: proto.Int64(3),
		Cmd:  invite,
	})})

	assert.Equal(t, 3, q.Len())
}


func TestRecvEmptyPacket(t *testing.T) {
	q := new(vector.Vector)

	recvPacket(q, Packet{"x", []byte{}})
	assert.Equal(t, 0, q.Len())
}


func TestRecvInvalidPacket(t *testing.T) {
	q := new(vector.Vector)
	recvPacket(q, Packet{"x", invalidProtobuf})
	assert.Equal(t, 0, q.Len())
}


func TestSchedTrigger(t *testing.T) {
	q := new(vector.Vector)
	d := int64(15e8)

	ts := time.Nanoseconds() + d
	schedTrigger(q, 1, d)

	assert.Equal(t, 1, q.Len())
	f, ok := q.At(0).(trigger)
	assert.Tf(t, ok, "expected a trigger, got a %T", q.At(0))
	assert.Equal(t, int64(1), f.n)
	assert.T(t, f.t >= ts)
}


func TestManagerPacketProcessing(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	in := make(chan Packet)
	out := make(chan Packet, 100)
	cfg := &Config{
		Alpha: 1,
		Store: st,
		In:    in,
		Out:   out,
		Ops:   st.Ops,
	}
	m := NewManager(cfg)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(node+"/a/addr", "x", 0),
	}
	for (<-m.Stats).TotalRuns < 1 {
	}

	in <- Packet{
		Data: mustMarshal(&msg{Seqn: proto.Int64(2), Cmd: learn, Value: []byte("foo")}),
		Addr: "127.0.0.1:9999",
	}
	assert.Equal(t, 0, (<-m.Stats).WaitPackets)

	for (<-m.Stats).TotalRuns < 2 {
	}
	assert.Equal(t, 1, (<-m.Stats).Runs)
}


func TestManagerDeletesSuccessfulRun(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	in := make(chan Packet)
	out := make(chan Packet, 100)
	cfg := &Config{
		Alpha: 1,
		Store: st,
		In:    in,
		Out:   out,
		Ops:   st.Ops,
	}
	m := NewManager(cfg)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(node+"/a/addr", "x", 0),
	}
	for (<-m.Stats).TotalRuns < 1 {
	}

	in <- Packet{
		Data: mustMarshal(&msg{Seqn: proto.Int64(2), Cmd: learn, Value: []byte("foo")}),
		Addr: "127.0.0.1:9999",
	}
	for (<-m.Stats).TotalRuns < 2 {
	}
	assert.Equal(t, 1, (<-m.Stats).Runs)
}


func TestManagerTickQueue(t *testing.T) {
	ticker := make(chan int64)
	st := store.New()
	defer close(st.Ops)
	in := make(chan Packet)
	cfg := &Config{
		Alpha:  1,
		Store:  st,
		In:     in,
		Ticker: ticker,
		Out:    make(chan Packet, 100),
	}
	m := NewManager(cfg)
	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(node+"/a/addr", "x", 0),
	}
	for (<-m.Stats).Runs < 1 {
	}

	// get it to tick for seqn 2
	in <- Packet{Data: mustMarshal(&msg{Seqn: proto.Int64(2), Cmd: propose})}
	assert.Equal(t, 1, (<-m.Stats).WaitTicks)

	ticker <- time.Nanoseconds() + initialWaitBound*2
	assert.Equal(t, int64(1), (<-m.Stats).TotalTicks)
}


func TestManagerFilterPropSeqn(t *testing.T) {
	ps := make(chan int64, 100)
	st := store.New()
	defer close(st.Ops)

	cfg := &Config{
		DefRev: 2,
		Alpha:  1,
		Self:   "b",
		PSeqn:  ps,
		Store:  st,
	}
	NewManager(cfg)

	st.Ops <- store.Op{1, store.MustEncodeSet("/ctl/cal/0", "a", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/1", "b", 0)}
	st.Ops <- store.Op{3, store.Nop}
	st.Ops <- store.Op{4, store.Nop}
	assert.Equal(t, int64(3), <-ps)
	assert.Equal(t, int64(5), <-ps)

	st.Ops <- store.Op{5, store.Nop}
	st.Ops <- store.Op{6, store.Nop}
	assert.Equal(t, int64(7), <-ps)
}


func TestManagerProposalQueue(t *testing.T) {
	props := make(chan *Prop)

	st := store.New()
	out := make(chan Packet, 100)
	cfg := &Config{
		Store: st,
		Out:   out,
		Props: props,
	}
	m := NewManager(cfg)
	props <- &Prop{Seqn: 1, Mut: []byte("foo")}

	assert.Equal(t, 1, (<-m.Stats).WaitPackets)
}


func TestManagerFillQueue(t *testing.T) {
	props := make(chan *Prop)
	ticker := make(chan int64)

	st := store.New()
	out := make(chan Packet, 100)
	cfg := &Config{
		Store:  st,
		Out:    out,
		DefRev: 2,
		Alpha:  1,
		Props:  props,
		Ticker: ticker,
	}
	m := NewManager(cfg)
	props <- &Prop{Seqn: 9, Mut: []byte("foo")}
	assert.Equal(t, 6, (<-m.Stats).WaitFills)

	ticker <- time.Nanoseconds()
	assert.Equal(t, 7, (<-m.Stats).WaitPackets)
}


func TestApplyTriggers(t *testing.T) {
	packets := new(vector.Vector)
	triggers := new(vector.Vector)

	heap.Push(triggers, trigger{t: 1, n: 1})
	heap.Push(triggers, trigger{t: 2, n: 2})
	heap.Push(triggers, trigger{t: 3, n: 3})
	heap.Push(triggers, trigger{t: 4, n: 4})
	heap.Push(triggers, trigger{t: 5, n: 5})
	heap.Push(triggers, trigger{t: 6, n: 6})
	heap.Push(triggers, trigger{t: 7, n: 7})
	heap.Push(triggers, trigger{t: 8, n: 8})
	heap.Push(triggers, trigger{t: 9, n: 9})

	n := applyTriggers(packets, triggers, 5, &msg{Cmd: tick})
	assert.Equal(t, 5, n)

	expTriggers := new(vector.Vector)
	expPackets := new(vector.Vector)
	heap.Push(expPackets, packet{msg: msg{Cmd: tick, Seqn: proto.Int64(1)}})
	heap.Push(expPackets, packet{msg: msg{Cmd: tick, Seqn: proto.Int64(2)}})
	heap.Push(expPackets, packet{msg: msg{Cmd: tick, Seqn: proto.Int64(3)}})
	heap.Push(expPackets, packet{msg: msg{Cmd: tick, Seqn: proto.Int64(4)}})
	heap.Push(expPackets, packet{msg: msg{Cmd: tick, Seqn: proto.Int64(5)}})
	heap.Push(expTriggers, trigger{t: 6, n: 6})
	heap.Push(expTriggers, trigger{t: 7, n: 7})
	heap.Push(expTriggers, trigger{t: 8, n: 8})
	heap.Push(expTriggers, trigger{t: 9, n: 9})

	sort.Sort(packets)
	sort.Sort(triggers)
	sort.Sort(expPackets)
	sort.Sort(expTriggers)

	assert.Equal(t, expTriggers, triggers)
	assert.Equal(t, expPackets, packets)
}


func TestAddRun(t *testing.T) {
	const alpha = 2
	runs := make(map[int64]*run)
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(node+"/a/addr", "x", 0),
	}

	st.Ops <- store.Op{
		Seqn: 2,
		Mut:  store.MustEncodeSet(cal+"/1", "a", 0),
	}

	ch, err := st.Wait(store.Any, 2)
	if err != nil {
		panic(err)
	}

	pseqn := make(chan int64, 1)
	c := &Config{
		Alpha: alpha,
		Self:  "a",
		PSeqn: pseqn,
		Ops:   st.Ops,
		Out:   make(chan Packet),
	}
	m := &Manager{cfg: *c, run: runs}
	got := m.addRun(<-ch)

	exp := &run{
		self:  "a",
		seqn:  2 + alpha,
		cals:  []string{"a"},
		addr:  []string{"x"},
		ops:   st.Ops,
		out:   c.Out,
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

	assert.Equal(t, exp, got)
	assert.Equal(t, exp, runs[got.seqn])
	assert.Equal(t, 1, len(runs))
	assert.Equal(t, exp.seqn, <-pseqn)
}


func TestGetCalsFull(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{Seqn: 1, Mut: store.MustEncodeSet(cal+"/1", "a", 0)}
	st.Ops <- store.Op{Seqn: 2, Mut: store.MustEncodeSet(cal+"/2", "c", 0)}
	st.Ops <- store.Op{Seqn: 3, Mut: store.MustEncodeSet(cal+"/3", "b", 0)}
	<-st.Seqns

	assert.Equal(t, []string{"a", "b", "c"}, getCals(st))
}


func TestGetCalsPartial(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{Seqn: 1, Mut: store.MustEncodeSet(cal+"/1", "a", 0)}
	st.Ops <- store.Op{Seqn: 2, Mut: store.MustEncodeSet(cal+"/2", "", 0)}
	st.Ops <- store.Op{Seqn: 3, Mut: store.MustEncodeSet(cal+"/3", "", 0)}
	<-st.Seqns

	assert.Equal(t, []string{"a"}, getCals(st))
}


func TestGetAddrs(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{1, store.MustEncodeSet(node+"/1/addr", "x", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet(node+"/2/addr", "y", 0)}
	st.Ops <- store.Op{3, store.MustEncodeSet(node+"/3/addr", "z", 0)}
	<-st.Seqns

	addrs := getAddrs(st, []string{"1", "2", "3"})
	assert.Equal(t, []string{"x", "y", "z"}, addrs)
}
