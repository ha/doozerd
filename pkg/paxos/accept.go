package paxos

import (
	"fmt"
	"strconv"
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

func accept(me uint64, ins, outs chan msg) {
	var rnd, vrnd uint64
	var vval string

	ch, sent := make(chan int), 0
	for in := range ins {

		if in.to != me && in.to != 0 {
			continue
		}

		switch in.cmd {
		case "INVITE":
			bodyParts, err := splitBody(in.body, iNumParts)
			if err != nil {
				continue
			}

			i, err := strconv.Btoui64(bodyParts[iRnd], 10)
			if err != nil {
				continue
			}

			switch {
			case i <= rnd:
			case i > rnd:
				rnd = i

				reply := msg{
					cmd: "ACCEPT",
					to: in.from, // reply to the sender
					from: me,
					body: fmt.Sprintf("%d:%d:%s", i, vrnd, vval),
				}
				go func(reply msg) { outs <- reply; ch <- 1 }(reply)
				sent++
			}
		case "NOMINATE":
			bodyParts, err := splitBody(in.body, nNumParts)
			if err != nil {
				continue
			}

			if len(bodyParts) != nNumParts {
				continue
			}

			i, err := strconv.Btoui64(bodyParts[nRnd], 10)
			if err != nil {
				continue
			}

			// SUPER IMPT MAD PAXOS
			if i < rnd || i == vrnd {
				continue
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

	for x := 0; x < sent; x++ {
		<-ch
	}

	close(outs)
}
