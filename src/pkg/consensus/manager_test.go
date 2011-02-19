package consensus


import (
	"container/vector"
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

	m := NewManager(nil, nil, runs)

	runs <- &run{seqn: 1}
	runs <- &run{seqn: 2}
	runs <- &run{seqn: 3}

	assert.Equal(t, 3, (<-m).Runs)
}


func TestManagerPacketQueue(t *testing.T) {
	in := make(chan Packet)

	m := NewManager(in, nil, nil)

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


func TestManagerPacketProcessing(t *testing.T) {
	runs := make(chan *run)
	in := make(chan Packet)
	m := NewManager(in, nil, runs)

	run := run{seqn: 1}
	runs <- &run

	in <- Packet{
		Data: mustMarshal(&M{Seqn: proto.Int64(1), Cmd: learn, Value: []byte("foo")}),
		Addr: "127.0.0.1:9999",
	}

	<-m
	assert.Equal(t, true, run.learner.done)
}
