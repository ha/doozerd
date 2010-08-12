package paxos

import (
	"fmt"
)

const (
	iRnd = iota
	iNumParts
)

const (
	nRnd = iota
	nVal
	nNumParts
)

func acceptor(me uint64, ins chan Msg, outs Putter) {
	var rnd, vrnd uint64
	var vval string

	update := func(in Msg) {
		defer swallowContinue()

		if in.to != me && in.to != 0 {
			return
		}

		switch in.cmd {
		case "INVITE":
			bodyParts := splitExactly(in.body, iNumParts)

			i := dtoui64(bodyParts[iRnd])

			switch {
			case i <= rnd:
			case i > rnd:
				rnd = i

				reply := Msg{
					seqn: in.seqn,
					cmd: "RSVP",
					to: in.from, // reply to the sender
					from: me,
					body: fmt.Sprintf("%d:%d:%s", i, vrnd, vval),
				}
				outs.Put(reply)
			}
		case "NOMINATE":
			bodyParts := splitExactly(in.body, nNumParts)

			i := dtoui64(bodyParts[nRnd])

			// SUPER IMPT MAD PAXOS
			if i < rnd || i == vrnd {
				return
			}

			rnd = i
			vrnd = i
			vval = bodyParts[nVal]

			broadcast := Msg{
				seqn: in.seqn,
				cmd: "VOTE",
				from: me,
				to: 0,
				body: fmt.Sprintf("%d:%s", i, vval),
			}
			outs.Put(broadcast)
		}
	}

	for in := range ins {
		update(in)
	}
}
