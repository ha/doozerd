package paxos

import (
	"github.com/bmizerany/assert"
	pb "goprotobuf.googlecode.com/hg/proto"
	"testing"
)


type fakePutterFrom struct {
	m *M
}


func (p *fakePutterFrom) PutFrom(addr string, m *M) {
	p.m = m
}


type fakeWriterTo struct {
	addr string
	data []byte
}


func (w *fakeWriterTo) WriteTo(data []byte, addr string) {
	w.addr = addr
	w.data = data
}


func TestEncode(t *testing.T) {
	m := newLearn("hi")
	b, err := pb.Marshal(m)
	assert.Equal(t, nil, err)
	
	m.SetFrom(3)

	var w fakeWriterTo
	Encoder{&w}.PutTo(m, "foo")
	assert.Equal(t, "foo", w.addr)
	assert.Equal(t, b, w.data)
}


func TestDecode(t *testing.T) {
	m := newLearn("hi")
	m.SetSeqn(3)
	b, err := pb.Marshal(m)
	assert.Equal(t, nil, err)
	
	var p fakePutterFrom
	Decoder{&p}.WriteFrom("foo", b)
	assert.Equal(t, m, p.m)
}
