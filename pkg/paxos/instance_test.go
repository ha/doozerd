package paxos

import (
	"borg/assert"
	"log"
	"testing"
)

// Testing

type FakePutter []Putter

func (fp FakePutter) Put(m Msg) {
	for _, p := range fp {
		p.Put(m)
	}
}

func selfRefNewInstance(id, nNodes uint64, logger *log.Logger) *Instance {
	p := make([]Putter, 1)
	cx := fakeCluster{FakePutter(p), nNodes, int(id)}
	ins := NewInstance(cx, logger)
	p[0] = ins
	return ins
}

func TestStartAtLearn(t *testing.T) {
	ins := selfRefNewInstance(1, 1, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtLearnWithDuplicates(t *testing.T) {
	ins := selfRefNewInstance(1, 1, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestLearnWithQuorumOf2(t *testing.T) {
	ins := selfRefNewInstance(1, 3, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("2:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestValueCanBeCalledMoreThanOnce(t *testing.T) {
	ins := selfRefNewInstance(1, 1, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins := selfRefNewInstance(1, 1, logger)
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtCoord(t *testing.T) {
	ins := selfRefNewInstance(1, 1, logger)
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

type FuncPutter func (Msg)

func (f FuncPutter) Put(m Msg) {
	f(m)
}

func (f FuncPutter) Close() {
}

func (f FuncPutter) process(string) {
}

func TestAllowsDirectlyAddressedMessages(t *testing.T) {
	nMessages := 0
	ins := selfRefNewInstance(1, 1, logger)
	ins.cPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.aPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.lPutter = FuncPutter(func(m Msg) {
		nMessages++
	})

	ins.Put(m("1:1:VOTE:1:foo"))
	assert.Equal(t, 3, nMessages, "")
	ins.Close()
}

func TestAllowsBroadcastMessages(t *testing.T) {
	nMessages := 0
	ins := selfRefNewInstance(1, 1, logger)
	ins.cPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.aPutter = FuncPutter(func(m Msg) {
		nMessages++
	})
	ins.lPutter = FuncPutter(func(m Msg) {
		nMessages++
	})

	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, 3, nMessages, "")
	ins.Close()
}

func TestIgnoresUnwantedMessages(t *testing.T) {
	ins := selfRefNewInstance(1, 1, logger)
	ins.cPutter = FuncPutter(func(m Msg) {
		t.Fatalf("instance should ignore message %#v", m)
	})
	ins.aPutter = FuncPutter(func(m Msg) {
		t.Fatalf("instance should ignore message %#v", m)
	})
	ins.lPutter = FuncPutter(func(m Msg) {
		t.Fatalf("instance should ignore message %#v", m)
	})

	ins.Put(m("2:2:VOTE:1:foo"))
	ins.Close()
}

func TestMultipleInstances(t *testing.T) {
	ps := make([]Putter, 3)
	cxA := fakeCluster{PutWrapper{1, 1, FakePutter(ps)}, 3, 1}
	cxB := fakeCluster{PutWrapper{1, 2, FakePutter(ps)}, 3, 2}
	cxC := fakeCluster{PutWrapper{1, 3, FakePutter(ps)}, 3, 3}
	insA := NewInstance(cxA, logger)
	insB := NewInstance(cxB, logger)
	insC := NewInstance(cxC, logger)
	ps[0] = insA
	ps[1] = insB
	ps[2] = insC

	insA.Propose("bar")
	assert.Equal(t, "bar", insA.Value(), "")
	insA.Close()
	insB.Close()
	insC.Close()
}

//func TestDeadlock(t *testing.T) {
//	<-make(chan int)
//}
