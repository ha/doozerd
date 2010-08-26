package paxos

func acceptor(ins chan Message, outs Putter) {
	var rnd, vrnd uint64
	var vval string

	update := func(in Message) {
		defer swallowContinue()

		switch in.Cmd() {
		case "INVITE":
			i := InviteParts(in)

			switch {
			case i <= rnd:
			case i > rnd:
				rnd = i

				reply := NewRsvp(i, vrnd, vval)
				outs.Put(reply)
			}
		case "NOMINATE":
			i, v := NominateParts(in)

			// SUPER IMPT MAD PAXOS
			if i < rnd || i == vrnd {
				return
			}

			rnd = i
			vrnd = i
			vval = v

			broadcast := NewVote(i, vval)
			outs.Put(broadcast)
		}
	}

	for in := range ins {
		update(in)
	}
}
