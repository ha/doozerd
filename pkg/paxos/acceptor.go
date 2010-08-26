package paxos

const (
	iRnd = iota
	iNumParts
)

const (
	nRnd = iota
	nVal
	nNumParts
)

func acceptor(ins chan Message, outs Putter) {
	var rnd, vrnd uint64
	var vval string

	update := func(in Message) {
		defer swallowContinue()

		switch in.Cmd() {
		case "INVITE":
			bodyParts := splitExactly(in.Body(), iNumParts)

			i := dtoui64(bodyParts[iRnd])

			switch {
			case i <= rnd:
			case i > rnd:
				rnd = i

				reply := NewRsvp(i, vrnd, vval)
				outs.Put(reply)
			}
		case "NOMINATE":
			bodyParts := splitExactly(in.Body(), nNumParts)

			i := dtoui64(bodyParts[nRnd])

			// SUPER IMPT MAD PAXOS
			if i < rnd || i == vrnd {
				return
			}

			rnd = i
			vrnd = i
			vval = bodyParts[nVal]

			broadcast := NewVote(i, vval)
			outs.Put(broadcast)
		}
	}

	for in := range ins {
		update(in)
	}
}
