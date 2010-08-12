package paxos

import (
	"borg/assert"
	"testing"
)

type Putter interface {
	Put(m Msg)
}

type Instance struct {
	quorum uint64

	vin  chan string
	vout chan string


	// Coordinator
	cIns chan Msg

	// Acceptor
	aIns chan Msg

	// Learner
	lIns  chan Msg
}

func (ins *Instance) Put(m Msg) {
	go func() { ins.cIns <- m }()
	go func() { ins.aIns <- m }()
	go func() { ins.lIns <- m }()
}

func (ins *Instance) Value() string {
	return <-ins.vout
}

func NewInstance(quorum uint64) *Instance {
	return &Instance{
		quorum: quorum,
		vin: make(chan string),
		vout: make(chan string),
		cIns:  make(chan Msg),
		aIns:  make(chan Msg),
		lIns:  make(chan Msg),
	}
}

func (ins *Instance) Init(p Putter) {
	go coordinator(1, ins.quorum, 3, ins.vin, ins.cIns, p, make(chan int))
	go acceptor(2, ins.aIns, p)
	go func() {
		ins.vout <- learner(1, ins.lIns, func() {})
	}()
}

func (ins *Instance) Close() {
	close(ins.cIns)
	close(ins.aIns)
	close(ins.lIns)
}

func (ins *Instance) Propose(v string) {
	ins.vin <- v
}


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
