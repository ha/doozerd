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

// TODO temporary name
type C struct {
	crnd uint64
	quorum uint64
	modulus uint64

	tCh chan string
	ins chan Msg
	outs Putter
	clock chan int
	logger *log.Logger
}

func coordinator(crnd, quorum, modulus uint64, tCh chan string, ins chan Msg, outs Putter, clock chan int, logger *log.Logger) {
	c := NewC()
	c.crnd = crnd
	c.quorum = quorum
	c.modulus = modulus
	c.tCh = tCh
	c.ins = ins
	c.outs = outs
	c.clock = clock
	c.logger = logger

	target := <-tCh
	if target == "" && closed(tCh) {
		return
	}

	c.process(target)
}

func NewC() *C {
	return &C{}
}

func (c *C) process(target string) {
	//if c.crnd > c.modulus {
	//	panic(IdOutOfRange)
	//}

	var cval string

Start:
	cval = ""
	start := Msg{
		Cmd:  "INVITE",
		To:   0, // send to all acceptors
		Body: fmt.Sprintf("%d", c.crnd),
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

				if i < c.crnd {
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
						Body: fmt.Sprintf("%d:%s", c.crnd, v),
					}
					c.outs.Put(choosen)
				}
			}
		case <-c.clock:
			c.crnd += c.modulus
			goto Start
		}
	}

Done:
	return
}
