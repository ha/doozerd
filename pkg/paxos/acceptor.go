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

func acceptor(me uint64, ins, outs chan msg) {
	var rnd, vrnd uint64
	var vval string

	ch, sent := make(chan int), 0

	update := func(in msg) {
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

				reply := msg{
					cmd: "RSVP",
					to: in.from, // reply to the sender
					from: me,
					body: fmt.Sprintf("%d:%d:%s", i, vrnd, vval),
				}
				go func(reply msg) { outs <- reply; ch <- 1 }(reply)
				sent++
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

			broadcast := msg{
				cmd: "VOTE",
				from: me,
				to: 0,
				body: fmt.Sprintf("%d:%s", i, vval),
			}
			go func(broadcast msg) { outs <- broadcast; ch <- 1 }(broadcast)
			sent++
		}
	}

	for in := range ins {
		update(in)
	}

	for x := 0; x < sent; x++ {
		<-ch
	}

	close(outs)
}
