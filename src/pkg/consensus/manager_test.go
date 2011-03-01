package consensus


import (
	"container/vector"
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
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

	m := newManager("", 0, nil, nil, runs, nil, nil, 0)

	r1 := &run{seqn: 1}
	r2 := &run{seqn: 2}
	r3 := &run{seqn: 3}

	runs <- r1
	runs <- r2
	runs <- r3

	assert.Equal(t, 3, (<-m).Runs)
	assert.NotEqual(t, (chan<- int64)(nil), r1.ticks)
	assert.NotEqual(t, (chan<- int64)(nil), r2.ticks)
	assert.NotEqual(t, (chan<- int64)(nil), r3.ticks)
}


func TestManagerPacketQueue(t *testing.T) {
	in := make(chan Packet)

	m := newManager("", 0, nil, in, nil, nil, nil, 0)

	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(1)})}

	assert.Equal(t, 1, (<-m).WaitPackets)
}


func TestManagerDropsOldPackets(t *testing.T) {
	runs := make(chan *run)
	defer close(runs)

	in := make(chan Packet)
	m := newManager("", 0, nil, in, runs, nil, nil, 0)

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

func TestSchedTick(t *testing.T) {
	q := new(vector.Vector)

	schedTick(q, 1)

	assert.Equal(t, 1, q.Len())
	assert.Equal(t, packet{M: M{Seqn: proto.Int64(1), Cmd: tick}}, q.At(0))
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


func TestManagerPacketProcessing(t *testing.T) {
	runs := make(chan *run)
	defer close(runs)

	in := make(chan Packet)
	m := newManager("", 0, nil, in, runs, nil, nil, 0)

	run := run{seqn: 1, ops: make(chan store.Op, 100)}
	runs <- &run

	in <- Packet{
		Data: mustMarshal(&M{Seqn: proto.Int64(1), Cmd: learn, Value: []byte("foo")}),
		Addr: "127.0.0.1:9999",
	}

	<-m
	assert.Equal(t, true, run.l.done)
}


func TestManagerTick(t *testing.T) {
	runs := make(chan *run)
	defer close(runs)

	m := newManager("", 0, nil, nil, runs, nil, nil, 0)

	// get our hands on the ticks chan
	r := &run{seqn: 1}
	runs <- r
	<-m
	ticks := r.ticks

	// send it a tick for seqn 2
	ticks <- 2

	assert.Equal(t, 1, (<-m).WaitPackets)
}


func TestManagerFilterPropSeqn(t *testing.T) {
	ps := make(chan int64, 100)
	runs := make(chan *run)
	defer close(runs)

	go filterPropSeqns("b", runs, ps)

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

	m := newManager("", 0, nil, nil, nil, props, nil, 0)
	props <- &Prop{Seqn: 1, Mut: []byte("foo")}

	assert.Equal(t, 1, (<-m).WaitPackets)
}


func TestManagerFillQueue(t *testing.T) {
	props := make(chan *Prop)
	ticker := make(chan int64)

	m := newManager("", 3, nil, nil, nil, props, ticker, 0)
	props <- &Prop{Seqn: 9, Mut: []byte("foo")}

	assert.Equal(t, 6, (<-m).WaitFills)
	
	ticker <- time.Nanoseconds()
	
	assert.Equal(t, 7, (<-m).WaitPackets)
}
