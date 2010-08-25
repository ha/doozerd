package paxos

import (
	"borg/assert"
	"testing"
)

type fakeCluster struct {
	outs Putter
	length uint64
	selfIndex int
}

func (f fakeCluster) Put(m Msg) {
	f.outs.Put(m)
}

func (f fakeCluster) Len() int {
	return int(f.length)
}

func (f fakeCluster) Quorum() int {
	return f.Len()/2 + 1
}

func (f fakeCluster) SelfIndex() int {
	return f.selfIndex
}

func TestCoordPut(t *testing.T) {
	c := NewC(fakeCluster{nil, 0, 0})
	c.ins = make(chan Msg)
	msg := m("1:1:RSVP:1:0")
	c.Put(msg)
	assert.Equal(t, msg, <-c.ins, "")
}

func TestCoordIgnoreOldMessages(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	nNodes := uint64(10)
	c := NewC(fakeCluster{outs, nNodes, 1})
	go func() {
		c.process("foo")
		done <- 1
	}()

	<-outs //discard INVITE:1

	c.clock <- 1 // force the start of a new round
	<-outs     //discard INVITE:11

	c.Put(m("1:1:RSVP:1:0:"))
	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:0:"))

	c.Close()
	assert.Equal(t, 1, <-done, "")

	close(outs)
}

func TestCoordCloseIns(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	nNodes := uint64(10)
	c := NewC(fakeCluster{outs, nNodes, 1})
	go func() {
		c.process("foo")
		done <- 1
	}()

	<-outs //discard INVITE:1

	c.clock <- 1 // force the start of a new round
	<-outs     //discard INVITE:11

	c.Put(m("1:1:RSVP:1:0:"))
	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:0:"))

	close(c.ins)
	assert.Equal(t, 1, <-done, "")

	close(outs)
}

func TestCoordCloseClock(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	nNodes := uint64(10)
	c := NewC(fakeCluster{outs, nNodes, 1})
	go func() {
		c.process("foo")
		done <- 1
	}()

	<-outs //discard INVITE:1

	c.clock <- 1 // force the start of a new round
	<-outs     //discard INVITE:11

	c.Put(m("1:1:RSVP:1:0:"))
	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:0:"))

	close(c.clock)
	assert.Equal(t, 1, <-done, "")

	close(outs)
}

func TestCoordStart(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	nNodes := uint64(10) // this is arbitrary

	c := NewC(fakeCluster{PutWrapper{1, 1, outs}, nNodes, 1})
	go c.process("foo")

	assert.Equal(t, m("1:*:INVITE:1"), <-outs, "")

	c.Close()
	close(outs)
}

// This is here mainly for triangulation.  It ensures we're not
// hardcoding crnd.
func TestCoordStartAlt(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	nNodes := uint64(10) // this is arbitrary

	c := NewC(fakeCluster{PutWrapper{1, 2, outs}, nNodes, 2})
	go c.process("foo")

	assert.Equal(t, m("2:*:INVITE:2"), <-outs, "")

	close(outs)
}

func TestCoordTargetNomination(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	nNodes := uint64(10) // this is arbitrary
	c := NewC(fakeCluster{PutWrapper{1, 1, outs}, nNodes, 1})
	go c.process("foo")
	<-outs //discard INVITE

	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:0:"))
	c.Put(m("7:1:RSVP:1:0:"))
	assert.Equal(t, m("1:*:NOMINATE:1:foo"), <-outs, "")

	c.Close()
	close(outs)
}

func TestCoordRestart(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	nNodes := uint64(10) // this is arbitrary
	c := NewC(fakeCluster{PutWrapper{1, 1, outs}, nNodes, 1})
	go c.process("foo")
	<-outs //discard INVITE

	// never reach majority (force timeout)
	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:0:"))

	c.clock <- 1
	assert.Equal(t, m("1:*:INVITE:11"), <-outs, "")

	c.Close()
	close(outs)
}

func TestCoordNonTargetNomination(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	nNodes := uint64(10) // this is arbitrary
	c := NewC(fakeCluster{PutWrapper{1, 1, outs}, nNodes, 1})
	go c.process("foo")
	<-outs //discard INVITE

	c.Put(m("1:1:RSVP:1:0:"))
	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:1:bar"))
	assert.Equal(t, m("1:*:NOMINATE:1:bar"), <-outs, "")

	c.Close()
	close(outs)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	c := NewC(fakeCluster{PutWrapper{1, 1, outs}, nNodes, 1})
	go func() {
		go c.process("foo")
		done <- 1
	}()

	<-outs //discard INVITE

	c.Put(m("1:1:RSVP:1:0:"))
	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:0:"))
	assert.Equal(t, m("1:*:NOMINATE:1:foo"), <-outs, "")

	c.Put(m("7:1:RSVP:1:0:"))
	c.Close()
	assert.Equal(t, 1, <-done, "")

	c.Close()
	close(outs)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	nNodes := uint64(10) // this is arbitrary
	c := NewC(fakeCluster{PutWrapper{1, 1, outs}, nNodes, 1})
	go c.process("foo")
	<-outs //discard INVITE

	c.Put(m("1:1:RSVP:1:0:"))
	c.Put(m("2:1:RSVP:1:0:"))
	c.Put(m("3:1:RSVP:1:0:"))
	c.Put(m("4:1:RSVP:1:0:"))
	c.Put(m("5:1:RSVP:1:0:"))
	c.Put(m("6:1:RSVP:1:0:"))
	<-outs //discard NOMINATE

	c.clock <- 1 // force the start of a new round
	<-outs     //discard INVITE:11

	c.Put(m("1:1:RSVP:11:0:"))
	c.Put(m("2:1:RSVP:11:0:"))
	c.Put(m("3:1:RSVP:11:0:"))
	c.Put(m("4:1:RSVP:11:0:"))
	c.Put(m("5:1:RSVP:11:0:"))
	c.Put(m("6:1:RSVP:11:0:"))

	exp := m("1:*:NOMINATE:11:foo")
	assert.Equal(t, exp, <-outs, "")

	c.Close()
	close(outs)
}
