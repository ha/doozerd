package consensus


import (
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
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
	in := make(chan packet)

	m := NewManager(in, nil, nil)

	in <- packet{"127.0.0.1:9999", M{WireSeqn: proto.Int64(1)}}
	in <- packet{"127.0.0.1:9999", M{WireSeqn: proto.Int64(2)}}
	in <- packet{"127.0.0.1:9999", M{WireSeqn: proto.Int64(3)}}

	assert.Equal(t, 3, (<-m).WaitPackets)
}


func TestManagerPacketProcessing(t *testing.T) {
	runs := make(chan *Run)
	in := make(chan packet)
	m := NewManager(in, nil, runs)

	run := Run{Seqn: 1}
	runs <- &run

	in <- packet{
		M:    M{WireSeqn: proto.Int64(1), WireCmd: learn, Value: []byte("foo")},
		Addr: "127.0.0.1:9999",
	}

	<-m
	assert.Equal(t, true, run.learner.done)
}
