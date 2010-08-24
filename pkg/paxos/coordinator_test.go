package paxos

import (
	"borg/assert"
	"testing"
)

func TestCoordPut(t *testing.T) {
	c := NewC(FakeCluster{nil, 0})
	c.ins = make(chan Msg)
	msg := m("1:1:RSVP:1:0")
	c.Put(msg)
	assert.Equal(t, msg, <-c.ins, "")
}

func TestCoordIgnoreOldMessages(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	nNodes := uint64(10)
	c := NewC(FakeCluster{outs, nNodes})
	go func() {
		c.process("foo", 1)
		done <- 1
	}()

	<-outs //discard INVITE:1

	c.clock <- 1 // force the start of a new round
	<-outs     //discard INVITE:11

	c.ins <- m("1:1:RSVP:1:0:")
	c.ins <- m("2:1:RSVP:1:0:")
	c.ins <- m("3:1:RSVP:1:0:")
	c.ins <- m("4:1:RSVP:1:0:")
	c.ins <- m("5:1:RSVP:1:0:")
	c.ins <- m("6:1:RSVP:1:0:")

	close(c.ins)
	assert.Equal(t, 1, <-done, "")

	close(c.ins)
	close(outs)
	close(c.clock)
}

func TestCoordStart(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)

	nNodes := uint64(10) // this is arbitrary

	go coordinator(1, UNUSED, nNodes, tCh, ins, PutWrapper{1, 1, outs}, clock, logger)
	tCh <- "foo"

	assert.Equal(t, m("1:*:INVITE:1"), <-outs, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}

// This is here mainly for triangulation.  It ensures we're not
// hardcoding crnd.
func TestCoordStartAlt(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)

	nNodes := uint64(10) // this is arbitrary

	go coordinator(2, UNUSED, nNodes, tCh, ins, PutWrapper{1, 2, outs}, clock, logger)
	tCh <- "foo"

	assert.Equal(t, m("2:*:INVITE:2"), <-outs, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}

func TestCoordTargetNomination(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, UNUSED, nNodes, tCh, ins, PutWrapper{1, 1, outs}, clock, logger)
	tCh <- "foo"
	<-outs //discard INVITE

	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")
	ins <- m("7:1:RSVP:1:0:")
	assert.Equal(t, m("1:*:NOMINATE:1:foo"), <-outs, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}

func TestCoordRestart(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, UNUSED, nNodes, tCh, ins, PutWrapper{1, 1, outs}, clock, logger)
	tCh <- "foo"
	<-outs //discard INVITE

	// never reach majority (force timeout)
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")

	clock <- 1
	assert.Equal(t, m("1:*:INVITE:11"), <-outs, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}

func TestCoordNonTargetNomination(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, UNUSED, nNodes, tCh, ins, PutWrapper{1, 1, outs}, clock, logger)
	tCh <- "foo"
	<-outs //discard INVITE

	ins <- m("1:1:RSVP:1:0:")
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:1:bar")
	assert.Equal(t, m("1:*:NOMINATE:1:bar"), <-outs, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)
	done := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go func() {
		coordinator(1, UNUSED, nNodes, tCh, ins, PutWrapper{1, 1, outs}, clock, logger)
		done <- 1
	}()

	tCh <- "foo"
	<-outs //discard INVITE

	ins <- m("1:1:RSVP:1:0:")
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")
	assert.Equal(t, m("1:*:NOMINATE:1:foo"), <-outs, "")

	ins <- m("7:1:RSVP:1:0:")
	close(ins)
	assert.Equal(t, 1, <-done, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, UNUSED, nNodes, tCh, ins, PutWrapper{1, 1, outs}, clock, logger)
	tCh <- "foo"
	<-outs //discard INVITE

	ins <- m("1:1:RSVP:1:0:")
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")
	<-outs //discard NOMINATE

	clock <- 1 // force the start of a new round
	<-outs     //discard INVITE:11

	ins <- m("1:1:RSVP:11:0:")
	ins <- m("2:1:RSVP:11:0:")
	ins <- m("3:1:RSVP:11:0:")
	ins <- m("4:1:RSVP:11:0:")
	ins <- m("5:1:RSVP:11:0:")
	ins <- m("6:1:RSVP:11:0:")

	close(ins)

	exp := m("1:*:NOMINATE:11:foo")
	assert.Equal(t, exp, <-outs, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}

func TestAbortIfNoProposal(t *testing.T) {
	ins := make(chan Msg)
	outs := SyncPutter(make(chan Msg))
	clock := make(chan int)
	tCh := make(chan string)

	done := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go func() {
		coordinator(1, UNUSED, nNodes, tCh, ins, outs, clock, logger)
		done <- 1
	}()

	close(tCh)

	assert.Equal(t, 1, <-done, "")

	close(ins)
	close(outs)
	close(clock)
	close(tCh)
}
