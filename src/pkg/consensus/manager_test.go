package consensus


import (
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


func TestManagerRuns(t *testing.T) {
	runs := make(chan *run)

	m := NewManager(nil, nil, runs)

	runs <- &run{seqn: 1}
	runs <- &run{seqn: 2}
	runs <- &run{seqn: 3}

	assert.Equal(t, 3, (<-m).Runs)
}


func TestManagerPacketQueue(t *testing.T) {
	in := make(chan packet)

	m := NewManager(in, nil, nil)

	in <- packet{"127.0.0.1:9999", M{Seqn: proto.Int64(1)}}
	in <- packet{"127.0.0.1:9999", M{Seqn: proto.Int64(2)}}
	in <- packet{"127.0.0.1:9999", M{Seqn: proto.Int64(3)}}

	assert.Equal(t, 3, (<-m).WaitPackets)
}


func TestManagerPacketProcessing(t *testing.T) {
	runs := make(chan *run)
	in := make(chan packet)
	m := NewManager(in, nil, runs)

	run := run{seqn: 1}
	runs <- &run

	in <- packet{
		M:    M{Seqn: proto.Int64(1), Cmd: learn, Value: []byte("foo")},
		Addr: "127.0.0.1:9999",
	}

	<-m
	assert.Equal(t, true, run.learner.done)
}
