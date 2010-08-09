package paxos

import (
	"borg/assert"
	"testing"

	"fmt"
	"os"
)

var (
	IdOutOfRange = os.NewError("Id Out of Range")
)

func coordinator(me, nNodes uint64, v string, ins, outs chan msg, clock chan int) {
	if me > nNodes {
		panic(IdOutOfRange)
	}

	var crnd uint64 = me

Start:
	start := msg{
		cmd:  "INVITE",
		to:   0, // send to all acceptors
		from: me,
		body: fmt.Sprintf("%d", crnd),
	}
	outs <- start

	var rsvps uint64

	quorum := nNodes/2 + 1

	for {
		select {
		case in := <-ins:
			if closed(ins) {
				close(outs)
				return
			}
			switch in.cmd {
			case "RSVP":
				rsvps++
				if rsvps >= quorum {
					choosen := msg{
						cmd:  "NOMINATE",
						to:   0, // send to all acceptors
						from: me,
						body: fmt.Sprintf("%d:%s", crnd, v),
					}
					go func() { outs <- choosen }()
				}
			}
		case <-clock:
			crnd += nNodes
			goto Start
		}
	}
}


// Testing

// This is here mainly for triangulation.  It ensures we're not
// hardcoding crnd.
func TestStartsRoundAtMe(t *testing.T) {
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

func TestPanicWhenMeIsOutOfRange(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	assert.Panic(t, IdOutOfRange, func() {
		coordinator(11, nNodes, "foo", ins, outs, clock)
	})
}

func TestPhase2aTimeoutStartsNewRound(t *testing.T) {
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

func TestShutdown(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)
	clock := make(chan int)

	nNodes := uint64(10) // this is arbitrary
	go coordinator(1, nNodes, "foo", ins, outs, clock)

	close(ins)

	exp := msgs("1:*:INVITE:1")
	assert.Equal(t, exp, gather(outs), "")
}
