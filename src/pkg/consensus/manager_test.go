package consensus


import (
	"container/vector"
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
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
	ops := make(chan<- store.Op)

	m := NewManager(nil, nil, runs, ops)

	r1 := &run{seqn: 1}
	r2 := &run{seqn: 2}
	r3 := &run{seqn: 3}

	runs <- r1
	runs <- r2
	runs <- r3

	assert.Equal(t, 3, (<-m).Runs)
	assert.Equal(t, ops, r1.ops)
	assert.Equal(t, ops, r2.ops)
	assert.Equal(t, ops, r3.ops)
}


func TestManagerPacketQueue(t *testing.T) {
	in := make(chan Packet)

	m := NewManager(in, nil, nil, nil)

	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(1)})}

	assert.Equal(t, 1, (<-m).WaitPackets)
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


func TestManagerPacketProcessing(t *testing.T) {
	runs := make(chan *run)
	in := make(chan Packet)
	m := NewManager(in, nil, runs, make(chan store.Op, 100))

	run := run{seqn: 1}
	runs <- &run

	in <- Packet{
		Data: mustMarshal(&M{Seqn: proto.Int64(1), Cmd: learn, Value: []byte("foo")}),
		Addr: "127.0.0.1:9999",
	}

	<-m
	assert.Equal(t, true, run.l.done)
}
