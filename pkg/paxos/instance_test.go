package paxos

import (
	"borg/assert"
	"testing"
)

// Testing

func TestStartAtLearn(t *testing.T) {
	ins := NewInstance(1)
	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestValueCanBeCalledMoreThanOnce(t *testing.T) {
	ins := NewInstance(1)
	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins := NewInstance(1)
	ins.Init(ins)
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtCoord(t *testing.T) {
	ins := NewInstance(1)
	ins.Init(ins)
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

type FakePutter []Putter

func (fp FakePutter) Put(m Msg) {
	for _, p := range fp {
		p.Put(m)
	}
}

func TestMultipleInstances(t *testing.T) {
	insA := NewInstance(2)
	insB := NewInstance(2)
	insC := NewInstance(2)
	ps := []Putter{insA, insB, insC}
	insA.Init(FakePutter(ps))
	insB.Init(FakePutter(ps))
	insC.Init(FakePutter(ps))

	insA.Propose("bar")
	assert.Equal(t, "bar", insA.Value(), "")
	insA.Close()
	insB.Close()
	insC.Close()
}

//func TestDeadlock(t *testing.T) {
//	<-make(chan int)
//}
