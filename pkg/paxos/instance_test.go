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

func NewInstance(p Putter, target string) *Instance {
	ins := Instance{
		cIns:  make(chan Msg),
		cOuts: make(chan Msg),
		aIns:  make(chan Msg),
		aOuts: make(chan Msg),
		lIns:  make(chan Msg),
		lOuts: make(chan string),
	}
	if target != "" {
		go coordinator(1, 1, 3, target, ins.cIns, ins.cOuts, make(chan int))
	}
	go acceptor(2, ins.aIns, ins.aOuts)
	go learner(1, ins.lIns, ins.lOuts, func() {})
	go func() {
		for m := range ins.cOuts {
			p.Put(m)
		}
	}()
	go func() {
		for m := range ins.aOuts {
			p.Put(m)
		}
	}()
	return &ins
}


// Testing

type FakeManager struct {
	p Putter
}

func (fm *FakeManager) Put(m Msg) {
	fm.p.Put(m)
}

func TestStartAtLearn(t *testing.T) {
	fm := &FakeManager{}
	ins := NewInstance(fm, "")
	fm.p = ins
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
}

func TestStartAtAccept(t *testing.T) {
	fm := &FakeManager{}
	ins := NewInstance(fm, "")
	fm.p = ins
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
}

func TestStartAtCoord(t *testing.T) {
	fm := &FakeManager{}
	ins := NewInstance(fm, "foo")
	fm.p = ins
	assert.Equal(t, "foo", ins.Value(), "")
}
