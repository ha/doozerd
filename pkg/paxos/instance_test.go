package paxos

import (
	"borg/assert"
	"testing"
)

type Putter interface {
	Put(m Msg)
}

type Instance struct {
	msgs chan Msg
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

func NewInstance() *Instance {
	return &Instance{
		vin: make(chan string),
		vout: make(chan string),
		msgs:  make(chan Msg),
		cIns:  make(chan Msg),
		aIns:  make(chan Msg),
		lIns:  make(chan Msg),
	}
}

func (ins *Instance) Init(p Putter) {
	go coordinator(1, 1, 3, ins.vin, ins.cIns, ins.msgs, make(chan int))
	go acceptor(2, ins.aIns, ins.msgs)
	go learner(1, ins.lIns, ins.vout, func() {})
	go func() {
		for m := range ins.msgs {
			p.Put(m)
		}
	}()
}

func (ins *Instance) Propose(v string) {
	ins.vin <- v
}


// Testing

func TestStartAtLearn(t *testing.T) {
	ins := NewInstance()
	ins.Init(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
}

func TestStartAtAccept(t *testing.T) {
	ins := NewInstance()
	ins.Init(ins)
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
}

func TestStartAtCoord(t *testing.T) {
	ins := NewInstance()
	ins.Init(ins)
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
}
