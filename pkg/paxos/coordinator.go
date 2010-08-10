package paxos

import (
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

func coordinator(me, quorum, nNodes uint64, target string, ins, outs chan Msg, clock chan int) {
	if me > nNodes {
		panic(IdOutOfRange)
	}

	var sent int
	var ch = make(chan int)

	var crnd uint64 = me
	var cval string

Start:
	cval = ""
	start := Msg{
		cmd:  "INVITE",
		to:   0, // send to all acceptors
		from: me,
		body: fmt.Sprintf("%d", crnd),
	}
	outs <- start

	var rsvps uint64
	var vr uint64
	var vv string

	for {
		fmt.Printf("coord: waiting for message or clock tick\n")
		select {
		case in := <-ins:
			fmt.Printf("coord: got message %#v\n", in)
			if closed(ins) {
				goto Done
			}
			switch in.cmd {
			case "RSVP":
				fmt.Printf("coord: got RSVP\n")
				bodyParts := splitExactly(in.body, rNumParts)
				i := dtoui64(bodyParts[rRnd])
				vrnd := dtoui64(bodyParts[rVrnd])
				vval := bodyParts[rVval]

				if cval != "" {
					fmt.Printf("coord: cval is empty\n")
					continue
				}

				if i < crnd {
					fmt.Printf("coord: i < crnd === %d < %d\n", i, crnd)
					continue
				}

				if vrnd > vr {
					vr = vrnd
					vv = vval
				}

				rsvps++
				fmt.Printf("coord: rsvps=%d quorum=%d\n", rsvps, quorum)
				if rsvps >= quorum {
					fmt.Printf("got quorum of RSVPs\n")
					var v string

					if vr > 0 {
						v = vv
					} else {
						v = target
					}
					cval = v

					choosen := Msg{
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
