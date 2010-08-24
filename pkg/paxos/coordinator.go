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
	Putter
	Len() int
	Quorum() int
}

// TODO this is temporarily here during refactoring. it should be moved to
// testing-only code when possible.
type fakeCluster struct {
	outs Putter
	length uint64
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

// TODO temporary name
type C struct {
	cluster Cluster

	ins chan Msg
	clock chan int
}

func coordinator(crnd, quorum, modulus uint64, tCh chan string, ins chan Msg, outs Putter, clock chan int, logger *log.Logger) {
	c := NewC(fakeCluster{outs, modulus})
	c.ins = ins
	c.clock = clock

	target := <-tCh
	if target == "" && closed(tCh) {
		return
	}

	// TODO this ugly cast will go away when we fix Msg
	c.process(target, int(crnd))
}

func NewC(c Cluster) *C {
	return &C{
		cluster: c,
		ins: make(chan Msg),
		clock: make(chan int),
	}
}

func (c *C) Put(m Msg) {
	go func() {
		c.ins <- m
	}()
}

func (c *C) Close() {
	close(c.ins)
	close(c.clock)
}

func (c *C) process(target string, crnd int) {
	//if crnd > c.cluster.Len() {
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
	c.cluster.Put(start)

	var rsvps int
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
				if rsvps >= c.cluster.Quorum() {
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
					c.cluster.Put(choosen)
				}
			}
		case <-c.clock:
			if closed(c.clock) {
				goto Done
			}
			crnd += c.cluster.Len()
			goto Start
		}
	}

Done:
	return
}
