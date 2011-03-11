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


func mustMarshal(p interface{}) []byte {
	buf, err := proto.Marshal(p)
	if err != nil {
		panic(err)
	}
	return buf
}


func TestManagerRuns(t *testing.T) {
	runs := make(chan *run)
	defer close(runs)

	st := store.New()
	out := make(chan Packet, 100)
	m := newManager("", 0, nil, nil, runs, nil, nil, 0, st, out)

	r1 := &run{seqn: 1}
	r2 := &run{seqn: 2}
	r3 := &run{seqn: 3}

	runs <- r1
	runs <- r2
	runs <- r3

	assert.Equal(t, 3, (<-m).Runs)
}


func TestManagerPacketQueue(t *testing.T) {
	in := make(chan Packet)

	st := store.New()
	out := make(chan Packet, 100)
	m := newManager("", 0, nil, in, nil, nil, nil, 0, st, out)

	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(1)})}

	assert.Equal(t, 1, (<-m).WaitPackets)
}


func TestManagerDropsOldPackets(t *testing.T) {
	runs := make(chan *run)
	defer close(runs)

	st := store.New()
	in := make(chan Packet)
	out := make(chan Packet, 100)
	m := newManager("", 0, nil, in, runs, nil, nil, 0, st, out)

	run := run{seqn: 2, ops: make(chan store.Op, 100)}
	runs <- &run

	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(1)})}

	assert.Equal(t, 0, (<-m).WaitPackets)
}


func TestRecvPacket(t *testing.T) {
	q := new(vector.Vector)

	recvPacket(q, Packet{"x", mustMarshal(&M{Seqn: proto.Int64(1)})})
	recvPacket(q, Packet{"x", mustMarshal(&M{Seqn: proto.Int64(2)})})
	recvPacket(q, Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3)})})

	assert.Equal(t, 3, q.Len())
}


func TestRecvEmptyPacket(t *testing.T) {
	q := new(vector.Vector)

	recvPacket(q, Packet{"x", []byte{}})
	assert.Equal(t, 0, q.Len())
}


func TestRecvInvalidPacket(t *testing.T) {
	q := new(vector.Vector)

	// The first element in a protobuf stream is always a varint.
	// The high bit of a varint byte indicates continuation;
	// Here we're supplying a continuation bit without a
	// subsequent byte. See also
	// http://code.google.com/apis/protocolbuffers/docs/encoding.html#varints.
	recvPacket(q, Packet{"x", []byte{0x80}})
	assert.Equal(t, 0, q.Len())
}


func TestSchedFill(t *testing.T) {
	q := new(vector.Vector)
	d := int64(15e8)

	ts := time.Nanoseconds() + d
	schedFill(q, 1, d)

	assert.Equal(t, 1, q.Len())
	f, ok := q.At(0).(fill)
	assert.Tf(t, ok, "expected a fill, got a %T", q.At(0))
	assert.Equal(t, int64(1), f.n)
	assert.T(t, f.t >= ts)
}


func TestSchedTick(t *testing.T) {
	q := new(vector.Vector)
	d := int64(15e8)

	ts := time.Nanoseconds() + d
	schedTick(q, 1, d)

	assert.Equal(t, 1, q.Len())
	f, ok := q.At(0).(tickTime)
	assert.Tf(t, ok, "expected a tickTime, got a %T", q.At(0))
	assert.Equal(t, int64(1), f.n)
	assert.T(t, f.t >= ts)
}


func TestManagerPacketProcessing(t *testing.T) {
	runs := make(chan *run)
	defer close(runs)

	st := store.New()
	in := make(chan Packet)
	out := make(chan Packet, 100)
	m := newManager("", 0, nil, in, runs, nil, nil, 0, st, out)

	run := run{seqn: 1, ops: make(chan store.Op, 100)}
	runs <- &run

	in <- Packet{
		Data: mustMarshal(&M{Seqn: proto.Int64(1), Cmd: learn, Value: []byte("foo")}),
		Addr: "127.0.0.1:9999",
	}

	<-m
	assert.Equal(t, true, run.l.done)
}


func TestManagerDeletesSuccessfulRun(t *testing.T) {
	runs := make(chan *run)
	defer close(runs)

	st := store.New()
	in := make(chan Packet)
	out := make(chan Packet, 100)
	m := newManager("", 0, nil, in, runs, nil, nil, 0, st, out)

	run := run{seqn: 1, ops: make(chan store.Op, 100)}
	runs <- &run

	in <- Packet{
		Data: mustMarshal(&M{Seqn: proto.Int64(1), Cmd: learn, Value: []byte("foo")}),
		Addr: "127.0.0.1:9999",
	}

	stat := <-m
	assert.Equal(t, 0, stat.Running)
}


func TestManagerTickQueue(t *testing.T) {
	ticker := make(chan int64)
	runs := make(chan *run)
	defer close(runs)

	st := store.New()
	defer close(st.Ops)
	in := make(chan Packet)
	m := newManager("", 0, nil, in, runs, nil, ticker, 0, st, nil)

	runs <- &run{seqn: 1}
	for (<-m).Runs < 1 {
	}

	// get it to tick for seqn 2
	in <- Packet{Data: mustMarshal(&M{Seqn: proto.Int64(1), Cmd: propose})}

	assert.Equal(t, 1, (<-m).WaitTicks)

	ticker <- time.Nanoseconds()

	assert.Equal(t, int64(1), (<-m).TotalTicks)
}


func TestManagerFilterPropSeqn(t *testing.T) {
	ps := make(chan int64, 100)
	runs := make(chan *run)
	defer close(runs)

	newManager("b", 0, ps, nil, runs, nil, nil, 0, nil, nil)

	runs <- &run{seqn: 3, cals: []string{"a", "b"}}
	runs <- &run{seqn: 4, cals: []string{"a", "b"}}
	runs <- &run{seqn: 5, cals: []string{"a", "b"}}
	assert.Equal(t, int64(3), <-ps)
	assert.Equal(t, int64(5), <-ps)

	runs <- &run{seqn: 6, cals: []string{"a", "b"}}
	runs <- &run{seqn: 7, cals: []string{"a", "b"}}
	assert.Equal(t, int64(7), <-ps)
}


func TestManagerProposalQueue(t *testing.T) {
	props := make(chan *Prop)

	st := store.New()
	out := make(chan Packet, 100)
	m := newManager("", 0, nil, nil, nil, props, nil, 0, st, out)
	props <- &Prop{Seqn: 1, Mut: []byte("foo")}

	assert.Equal(t, 1, (<-m).WaitPackets)
}


func TestManagerFillQueue(t *testing.T) {
	props := make(chan *Prop)
	ticker := make(chan int64)

	st := store.New()
	out := make(chan Packet, 100)
	m := newManager("", 3, nil, nil, nil, props, ticker, 0, st, out)
	props <- &Prop{Seqn: 9, Mut: []byte("foo")}

	assert.Equal(t, 6, (<-m).WaitFills)

	ticker <- time.Nanoseconds()

	assert.Equal(t, 7, (<-m).WaitPackets)
}


func TestApplyTicks(t *testing.T) {
	packets := new(vector.Vector)
	ticks := new(vector.Vector)

	heap.Push(ticks, tickTime{t: 1, n: 1})
	heap.Push(ticks, tickTime{t: 2, n: 2})
	heap.Push(ticks, tickTime{t: 3, n: 3})
	heap.Push(ticks, tickTime{t: 4, n: 4})
	heap.Push(ticks, tickTime{t: 5, n: 5})
	heap.Push(ticks, tickTime{t: 6, n: 6})
	heap.Push(ticks, tickTime{t: 7, n: 7})
	heap.Push(ticks, tickTime{t: 8, n: 8})
	heap.Push(ticks, tickTime{t: 9, n: 9})

	n := applyTicks(packets, ticks, 5)
	assert.Equal(t, 5, n)

	expTicks := new(vector.Vector)
	expPackets := new(vector.Vector)
	heap.Push(expPackets, packet{M: M{Cmd: tick, Seqn: proto.Int64(1)}})
	heap.Push(expPackets, packet{M: M{Cmd: tick, Seqn: proto.Int64(2)}})
	heap.Push(expPackets, packet{M: M{Cmd: tick, Seqn: proto.Int64(3)}})
	heap.Push(expPackets, packet{M: M{Cmd: tick, Seqn: proto.Int64(4)}})
	heap.Push(expPackets, packet{M: M{Cmd: tick, Seqn: proto.Int64(5)}})
	heap.Push(expTicks, tickTime{t: 6, n: 6})
	heap.Push(expTicks, tickTime{t: 7, n: 7})
	heap.Push(expTicks, tickTime{t: 8, n: 8})
	heap.Push(expTicks, tickTime{t: 9, n: 9})

	sort.Sort(packets)
	sort.Sort(ticks)
	sort.Sort(expPackets)
	sort.Sort(expTicks)

	assert.Equal(t, expTicks, ticks)
	assert.Equal(t, expPackets, packets)
}
