package paxos

import (
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
	var crnd int = c.cx.SelfIndex()
	if crnd == 0 {
		crnd += c.cx.Len()
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
				bodyParts := splitExactly(in.Body(), rNumParts)

				// TODO this ugly cast will go away when we fix Msg
				i := int(dtoui64(bodyParts[rRnd]))

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
			crnd += c.cx.Len()
			goto Start
		}
	}

Done:
	return
}
