package paxos

import (
	"borg/assert"
	"fmt"
	"testing"
)

type Putter interface {
	Put(m Msg)
}

type Instance struct {
	msgs  chan Msg

	// Coordinator
	cIns  chan Msg
	cOuts chan Msg

	// Acceptor
	aIns  chan Msg
	aOuts chan Msg

	// Learner
	lIns  chan Msg
	lOuts chan string
}

func (ins *Instance) Put(m Msg) {
	fmt.Printf("receiving message %#v\n", m)
	go func() { ins.cIns <- m }()
	go func() { ins.aIns <- m }()
	go func() { ins.lIns <- m }()
}

func (ins *Instance) Value() string {
	return <-ins.lOuts
}

func NewInstance() *Instance {
	ins := Instance{
		cIns:  make(chan Msg),
		cOuts: make(chan Msg),
		aIns:  make(chan Msg),
		aOuts: make(chan Msg),
		lIns:  make(chan Msg),
		lOuts: make(chan string),
	}
	return &ins
}

func (ins *Instance) Go(p Putter) {
	go acceptor(2, ins.aIns, ins.aOuts)
	go learner(1, ins.lIns, ins.lOuts, func() {})
	go func() {
		for m := range ins.aOuts {
			p.Put(m)
		}
	}()
	go func() {
		for m := range ins.cOuts {
			p.Put(m)
		}
	}()
}

func (ins *Instance) Propose(v string) {
	go coordinator(1, 1, 3, v, ins.cIns, ins.cOuts, make(chan int))
}


// Testing

func TestStartAtLearn(t *testing.T) {
	ins := NewInstance()
	ins.Go(ins)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
}

func TestStartAtAccept(t *testing.T) {
	ins := NewInstance()
	ins.Go(ins)
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
}

func TestStartAtCoord(t *testing.T) {
	ins := NewInstance()
	ins.Go(ins)
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
}
