package paxos

import (
	"borg/assert"
	"testing"
)

// Testing

func TestStartAtLearn(t *testing.T) {
	ins := NewInstance(1, 1, logger)
	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtLearnWithDuplicates(t *testing.T) {
	ins := NewInstance(1, 1, logger)
	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestLearnWithQuorumOf2(t *testing.T) {
	ins := NewInstance(1, 2, logger)
	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("2:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestValueCanBeCalledMoreThanOnce(t *testing.T) {
	ins := NewInstance(1, 1, logger)
	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins := NewInstance(1, 1, logger)
	ins.Init(ins)
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtCoord(t *testing.T) {
	ins := NewInstance(1, 1, logger)
	ins.Init(ins)
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

type FuncPutter func (Msg)

func (f FuncPutter) Put(m Msg) {
	f(m)
}

func TestAllowsDirectlyAddressedMessages(t *testing.T) {
	nMessages := 0
	ins := NewInstance(1, 1, logger)
	ins.cPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.aPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.lPutter = FuncPutter(func(m Msg) {
		nMessages++
	})

	ins.Init(ins)
	ins.Put(m("1:1:VOTE:1:foo"))
	assert.Equal(t, 3, nMessages, "")
	ins.Close()
}

func TestAllowsBroadcastMessages(t *testing.T) {
	nMessages := 0
	ins := NewInstance(1, 1, logger)
	ins.cPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.aPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.lPutter = FuncPutter(func(m Msg) {
		nMessages++
	})

	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, 3, nMessages, "")
	ins.Close()
}

func TestIgnoresUnwantedMessages(t *testing.T) {
	ins := NewInstance(1, 1, logger)
	ins.cPutter = FuncPutter(func(m Msg) {
		t.Fatalf("instance should ignore message %#v", m)
	})
	ins.aPutter = FuncPutter(func(m Msg) {
		t.Fatalf("instance should ignore message %#v", m)
	})
	ins.lPutter = FuncPutter(func(m Msg) {
		t.Fatalf("instance should ignore message %#v", m)
	})

	ins.Init(ins)
	ins.Put(m("2:2:VOTE:1:foo"))
	ins.Close()
}

type FakePutter []Putter

func (fp FakePutter) Put(m Msg) {
	for _, p := range fp {
		p.Put(m)
	}
}

func TestMultipleInstances(t *testing.T) {
	insA := NewInstance(1, 2, logger)
	insB := NewInstance(2, 2, logger)
	insC := NewInstance(3, 2, logger)
	ps := []Putter{insA, insB, insC}
	insA.Init(PutWrapper{1, 1, FakePutter(ps)})
	insB.Init(PutWrapper{1, 2, FakePutter(ps)})
	insC.Init(PutWrapper{1, 3, FakePutter(ps)})

	insA.Propose("bar")
	assert.Equal(t, "bar", insA.Value(), "")
	insA.Close()
	insB.Close()
	insC.Close()
}

//func TestDeadlock(t *testing.T) {
//	<-make(chan int)
//}
