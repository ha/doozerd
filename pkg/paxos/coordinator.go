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

func coordinator(me, quorum, nNodes uint64, tCh chan string, ins chan Msg, outs Putter, clock chan int) {
	if me > nNodes {
		panic(IdOutOfRange)
	}

	var crnd uint64 = me
	var cval string

	target := <-tCh

Start:
	cval = ""
	start := Msg{
		cmd:  "INVITE",
		to:   0, // send to all acceptors
		from: me,
		body: fmt.Sprintf("%d", crnd),
	}
	outs.Put(start)

	var rsvps uint64
	var vr uint64
	var vv string

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

					choosen := Msg{
						cmd:  "NOMINATE",
						to:   0, // send to all acceptors
						from: me,
						body: fmt.Sprintf("%d:%s", crnd, v),
					}
					outs.Put(choosen)
				}
			}
		case <-clock:
			crnd += nNodes
			goto Start
		}
	}

Done:
	return
}
