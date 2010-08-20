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

func coordinator(crnd, quorum, modulus uint64, tCh chan string, ins chan Msg, outs Putter, clock chan int, logger *log.Logger) {
	//if crnd > modulus {
	//	panic(IdOutOfRange)
	//}

	var cval string

	target := <-tCh
	if target == "" && closed(tCh) {
		return
	}

Start:
	cval = ""
	start := Msg{
		Cmd:  "INVITE",
		To:   0, // send to all acceptors
		Body: fmt.Sprintf("%d", crnd),
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
			logger.Logf("coord got %v", in)
			switch in.Cmd {
			case "RSVP":
				logger.Logf("coord got rsvp")
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
				logger.Logf("now %d rsvps")
				if rsvps >= quorum {
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
					outs.Put(choosen)
				}
			}
		case <-clock:
			crnd += modulus
			goto Start
		}
	}

Done:
	return
}
