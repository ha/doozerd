package paxos

import (
	"os"
)

var (
	IdOutOfRange = os.NewError("Id Out of Range")
)

// TODO temporary name
type C struct {
	cx Cluster

	ins chan Message
	clock chan int
}

func NewC(c Cluster) *C {
	return &C{
		cx: c,
		ins: make(chan Message),
		clock: make(chan int),
	}
}

func (c *C) Put(m Message) {
	go func() {
		c.ins <- m
	}()
}

func (c *C) Close() {
	close(c.ins)
	close(c.clock)
}

func (c *C) process(target string) {
	crnd := uint64(c.cx.SelfIndex())
	if crnd == 0 {
		crnd += uint64(c.cx.Len())
	}

	//if crnd > c.cx.Len() {
	//	panic(IdOutOfRange)
	//}

	var cval string

Start:
	cval = ""
	start := NewInvite(crnd)
	c.cx.Put(start)

	var rsvps int
	var vr uint64
	var vv string

	for {
		select {
		case in := <-c.ins:
			if closed(c.ins) {
				goto Done
			}
			switch in.Cmd() {
			case "RSVP":
				i, vrnd, vval := RsvpParts(in)

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
				if rsvps >= c.cx.Quorum() {
					var v string

					if vr > 0 {
						v = vv
					} else {
						v = target
					}
					cval = v

					chosen := NewNominate(crnd, v)
					c.cx.Put(chosen)
				}
			}
		case <-c.clock:
			if closed(c.clock) {
				goto Done
			}
			crnd += uint64(c.cx.Len())
			goto Start
		}
	}

Done:
	return
}
