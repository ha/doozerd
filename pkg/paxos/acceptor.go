package paxos

import (
	"junta/util"
)

func acceptor(ins chan Msg, outs Putter) {
	logger := util.NewLogger("acceptor")
	var rnd, vrnd uint64
	var vval string

	for in := range ins {
		switch in.Cmd() {
		case invite:
			logger.Log("got invite", in)
			i := inviteParts(in)

			switch {
			case i <= rnd:
			case i > rnd:
				rnd = i

				reply := newRsvp(i, vrnd, vval)
				logger.Log("sending rsvp", reply)
				outs.Put(reply)
			}
		case nominate:
			logger.Log("got nom", in)
			i, v := nominateParts(in)

			// SUPER IMPT MAD PAXOS
			if i < rnd || i == vrnd {
				continue
			}

			rnd = i
			vrnd = i
			vval = v

			broadcast := newVote(i, vval)
			logger.Log("sending vote", broadcast)
			outs.Put(broadcast)
		}
	}
}
