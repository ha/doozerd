package paxos

import (
	"fmt"
	"log"
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

// TODO maybe we can make a better name for this. Not sure.
type Cluster interface {
	Len() int
	Quorum() int
}

// TODO this is temporary during refactoring. we should remove it when we can.
type FakeCluster struct {
	length uint64
	quorum uint64
}

func (f FakeCluster) Len() int {
	return int(f.length)
}

func (f FakeCluster) Quorum() int {
	return int(f.quorum)
}

// TODO temporary name
type C struct {
	quorum uint64
	modulus uint64

	ins chan Msg
	outs Putter
	clock chan int
}

func coordinator(crnd, quorum, modulus uint64, tCh chan string, ins chan Msg, outs Putter, clock chan int, logger *log.Logger) {
	c := NewC(FakeCluster{modulus, quorum})
	c.ins = ins
	c.outs = outs
	c.clock = clock

	target := <-tCh
	if target == "" && closed(tCh) {
		return
	}

	c.process(target, crnd)
}

func NewC(c Cluster) *C {
	return &C{
		quorum: uint64(c.Quorum()),
		modulus: uint64(c.Len()),
	}
}

func (c *C) process(target string, crnd uint64) {
	//if crnd > c.modulus {
	//	panic(IdOutOfRange)
	//}

	var cval string

Start:
	cval = ""
	start := Msg{
		Cmd:  "INVITE",
		To:   0, // send to all acceptors
		Body: fmt.Sprintf("%d", crnd),
	}
	c.outs.Put(start)

	var rsvps uint64
	var vr uint64
	var vv string

	for {
		select {
		case in := <-c.ins:
			if closed(c.ins) {
				goto Done
			}
			switch in.Cmd {
			case "RSVP":
				bodyParts := splitExactly(in.Body, rNumParts)
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
				if rsvps >= c.quorum {
					var v string

					if vr > 0 {
						v = vv
					} else {
						v = target
					}
					cval = v

					choosen := Msg{
						Cmd:  "NOMINATE",
						To:   0, // send to all acceptors
						Body: fmt.Sprintf("%d:%s", crnd, v),
					}
					c.outs.Put(choosen)
				}
			}
		case <-c.clock:
			crnd += c.modulus
			goto Start
		}
	}

Done:
	return
}
