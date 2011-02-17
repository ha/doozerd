package consensus


import (
	"goprotobuf.googlecode.com/hg/proto"
	"github.com/bmizerany/assert"
	"testing"
)


func TestManagerRuns(t *testing.T) {
	runs := make(chan *Run)

	m := NewManager(nil, nil, runs)

	runs <- &Run{Seqn: 1}
	runs <- &Run{Seqn: 2}
	runs <- &Run{Seqn: 3}

	assert.Equal(t, 3, (<-m).Runs)
}


func TestManagerPacketQueue(t *testing.T) {
	in := make(chan Packet)

	m := NewManager(in, nil, nil)

	in <- Packet{M{WireSeqn: proto.Int64(1)}, "127.0.0.1:9999"}
	in <- Packet{M{WireSeqn: proto.Int64(2)}, "127.0.0.1:9999"}
	in <- Packet{M{WireSeqn: proto.Int64(3)}, "127.0.0.1:9999"}

	assert.Equal(t, 3, (<-m).WaitPackets)
}


func TestManagerPacketProcessing(t *testing.T) {
	runs := make(chan *Run)
	in := make(chan Packet)
	m := NewManager(in, nil, runs)

	run := Run{Seqn: 1}
	runs <- &run

	in <- Packet{
		M: M{WireSeqn: proto.Int64(1), WireCmd: learn, Value: []byte("foo")},
		Addr: "127.0.0.1:9999",
	}

	<-m
	assert.Equal(t, true, run.sink.done)
}
