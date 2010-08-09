package paxos

import (
	"borg/assert"
	"testing"

	"fmt"
	"os"
)

const (
	rRnd = iota
	rVrnd
	rVval
	rNumParts
)

var (
	IdOutOfRange = os.NewError("Id Out of Range")
)

func coordinator(me, nNodes uint64, target string, ins, outs chan msg, clock chan int) {
	if me > nNodes {
		panic(IdOutOfRange)
	}

	var sent int
	var ch = make(chan int)

	var crnd uint64 = me
	var cval string

Start:
	cval = ""
	start := msg{
		cmd:  "INVITE",
		to:   0, // send to all acceptors
		from: me,
		body: fmt.Sprintf("%d", crnd),
	}
	outs <- start

	var rsvps uint64
	var vr uint64
	var vv string

	quorum := nNodes/2 + 1

	for {
		select {
		case in := <-ins:
			if closed(ins) {
				goto Done
			}
			switch in.cmd {
			case "RSVP":
				bodyParts := splitExactly(in.body, rNumParts)
				i := dtoui64(bodyParts[rRnd])
				vrnd := dtoui64(bodyParts[rVrnd])
				vval := bodyParts[rVval]

				if cval != "" {
					continue
				}

				if i < crnd {
					continue
				}

				if vrnd > vr {
					vr = vrnd
					vv = vval
				}

				rsvps++
				if rsvps >= quorum {
					var v string

					if vr > 0 {
						v = vv
					} else {
						v = target
					}
					cval = v

					choosen := msg{
						cmd:  "NOMINATE",
						to:   0, // send to all acceptors
						from: me,
						body: fmt.Sprintf("%d:%s", crnd, v),
					}
					go func() { outs <- choosen ; ch <- 1 }()
					sent++
				}
			}
		case <-clock:
			crnd += nNodes
			goto Start
		}
	}

Done:
	for x := 0; x < sent; x++ {
		<-ch
	}

	close(outs)
	return
}


// Testing

func TestCoordIgnoreOldMessages(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)
	<-outs //discard INVITE:1

	clock <- 1 // force the start of a new round
	<-outs //discard INVITE:11

	ins <- m("1:1:RSVP:1:0:")
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")

	close(ins)

	exp := []msg{}
	assert.Equal(t, exp, gather(outs), "")
}

// This is here mainly for triangulation.  It ensures we're not
// hardcoding crnd.
func TestCoordStart(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary

	res := make([]msg, 2)
	go coordinator(1, nNodes, "foo", ins, outs, clock)
	res[0] = <-outs
	go coordinator(2, nNodes, "foo", ins, outs, clock)
	res[1] = <-outs

	exp := msgs("1:*:INVITE:1", "2:*:INVITE:2")

	assert.Equal(t, exp, res, "")
}

func TestCoordIdOutOfRange(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	assert.Panic(t, IdOutOfRange, func() {
		coordinator(11, nNodes, "foo", ins, outs, clock)
	})
}

func TestCoordTargetNomination(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)
	<-outs //discard INVITE

	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")
	ins <- m("7:1:RSVP:1:0:")

	exp := m("1:*:NOMINATE:1:foo")
	assert.Equal(t, exp, <-outs, "")
}

func TestCoordRestart(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)
	<-outs //discard INVITE

	// never reach majority (force timeout)
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")

	clock <- 1

	exp := m("1:*:INVITE:11")
	assert.Equal(t, exp, <-outs, "")
}

func TestCoordShutdown(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)

	close(ins)

	exp := msgs("1:*:INVITE:1")
	assert.Equal(t, exp, gather(outs), "")
}

func TestCoordNonTargetNomination(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)
	<-outs //discard INVITE

	ins <- m("1:1:RSVP:1:0:")
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:1:bar")

	exp := m("1:*:NOMINATE:1:bar")
	assert.Equal(t, exp, <-outs, "")
}

func TestCoordOneNominationPerRound(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)
	<-outs //discard INVITE

	ins <- m("1:1:RSVP:1:0:")
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")
	ins <- m("7:1:RSVP:1:0:")
	close(ins)

	exp := msgs("1:*:NOMINATE:1:foo")
	assert.Equal(t, exp, gather(outs), "")
}

func TestCoordEachRoundResetsCval(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)
	<-outs //discard INVITE

	ins <- m("1:1:RSVP:1:0:")
	ins <- m("2:1:RSVP:1:0:")
	ins <- m("3:1:RSVP:1:0:")
	ins <- m("4:1:RSVP:1:0:")
	ins <- m("5:1:RSVP:1:0:")
	ins <- m("6:1:RSVP:1:0:")
	<-outs //discard NOMINATE

	clock <- 1 // force the start of a new round
	<-outs //discard INVITE:11

	ins <- m("1:1:RSVP:11:0:")
	ins <- m("2:1:RSVP:11:0:")
	ins <- m("3:1:RSVP:11:0:")
	ins <- m("4:1:RSVP:11:0:")
	ins <- m("5:1:RSVP:11:0:")
	ins <- m("6:1:RSVP:11:0:")

	close(ins)

	exp := msgs("1:*:NOMINATE:11:foo")
	assert.Equal(t, exp, gather(outs), "")
}
