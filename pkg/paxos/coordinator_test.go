package paxos

import (
	"junta/assert"
	"testing"
)

func TestCoordIgnoreOldMessages(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	c := make(ChanPutCloser)
	go func() {
		coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
		done <- 1
	}()
	c.Put(newPropose("foo"))

	<-outs //discard INVITE:1

	c.Put(newTick()) // force the start of a new round
	<-outs           //discard INVITE:11

	c.Put(newRsvpFrom(1, 1, 0, ""))
	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 0, ""))

	c.Close()
	assert.Equal(t, 1, <-done, "")

	close(outs)
}

func TestCoordCloseIns(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	c := make(ChanPutCloser)
	go func() {
		coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
		done <- 1
	}()
	c.Put(newPropose("foo"))

	<-outs //discard INVITE:1

	c.Put(newTick()) // force the start of a new round
	<-outs           //discard INVITE:11

	c.Put(newRsvpFrom(1, 1, 0, ""))
	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 0, ""))

	c.Close()
	assert.Equal(t, 1, <-done, "")

	close(outs)
}

func TestCoordStart(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	c := make(ChanPutCloser)
	go coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
	c.Put(newPropose("foo"))

	assert.Equal(t, newInvite(1), <-outs, "")

	c.Close()
	close(outs)
}

// This is here mainly for triangulation.  It ensures we're not
// hardcoding crnd.
func TestCoordStartAlt(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	c := make(ChanPutCloser)
	go coordinator(c, newCluster("c", tenNodes, tenIds, nil), outs)
	c.Put(newPropose("foo"))

	assert.Equal(t, newInvite(2), <-outs, "")

	close(outs)
}

func TestCoordTargetNomination(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	c := make(ChanPutCloser)
	go coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
	c.Put(newPropose("foo"))
	<-outs //discard INVITE

	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 0, ""))
	c.Put(newRsvpFrom(7, 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), <-outs, "")

	c.Close()
	close(outs)
}

func TestCoordRestart(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	c := make(ChanPutCloser)
	go coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
	c.Put(newPropose("foo"))
	<-outs //discard INVITE

	// never reach majority (force timeout)
	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 0, ""))

	c.Put(newTick()) // force the start of a new round
	assert.Equal(t, newInvite(11), <-outs, "")

	c.Close()
	close(outs)
}

func TestCoordNonTargetNomination(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	c := make(ChanPutCloser)
	go coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
	c.Put(newPropose("foo"))
	<-outs //discard INVITE

	c.Put(newRsvpFrom(1, 1, 0, ""))
	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 1, "bar"))
	assert.Equal(t, newNominate(1, "bar"), <-outs, "")

	c.Close()
	close(outs)
}

func TestCoordOneNominationPerRound(t *testing.T) {
	outs := SyncPutter(make(chan Msg))
	done := make(chan int)

	c := make(ChanPutCloser)
	go func() {
		go coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
		done <- 1
	}()
	c.Put(newPropose("foo"))

	<-outs //discard INVITE

	c.Put(newRsvpFrom(1, 1, 0, ""))
	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 0, ""))
	assert.Equal(t, newNominate(1, "foo"), <-outs, "")

	c.Put(newRsvpFrom(7, 1, 0, ""))
	c.Close()
	assert.Equal(t, 1, <-done, "")

	c.Close()
	close(outs)
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	outs := SyncPutter(make(chan Msg))

	c := make(ChanPutCloser)
	go coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)
	c.Put(newPropose("foo"))
	<-outs //discard INVITE

	c.Put(newRsvpFrom(1, 1, 0, ""))
	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 0, ""))
	<-outs //discard NOMINATE

	c.Put(newTick()) // force the start of a new round
	<-outs           //discard INVITE:11

	c.Put(newRsvpFrom(1, 11, 0, ""))
	c.Put(newRsvpFrom(2, 11, 0, ""))
	c.Put(newRsvpFrom(3, 11, 0, ""))
	c.Put(newRsvpFrom(4, 11, 0, ""))
	c.Put(newRsvpFrom(5, 11, 0, ""))
	c.Put(newRsvpFrom(6, 11, 0, ""))

	exp := newNominate(11, "foo")
	assert.Equal(t, exp, <-outs, "")

	c.Close()
	close(outs)
}

func TestCoordStartRsvp(t *testing.T) {
	outs := SyncPutter(make(chan Msg, 1))

	c := make(ChanPutCloser)
	go coordinator(c, newCluster("b", tenNodes, tenIds, nil), outs)

	c.Put(newRsvpFrom(1, 1, 0, ""))
	c.Put(newRsvpFrom(2, 1, 0, ""))
	c.Put(newRsvpFrom(3, 1, 0, ""))
	c.Put(newRsvpFrom(4, 1, 0, ""))
	c.Put(newRsvpFrom(5, 1, 0, ""))
	c.Put(newRsvpFrom(6, 1, 0, ""))

	c.Put(newPropose("foo"))

	// If the RSVPs were ignored, this will be an invite. Otherwise, it'll be a
	// nominate.
	assert.Equal(t, newInvite(1), <-outs, "")

	c.Close()
	close(outs)
}
