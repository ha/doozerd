package paxos

func acceptor(ins chan Msg, outs Putter) {
	var rnd, vrnd uint64
	var vval string

	for in := range ins {
		switch in.Cmd() {
		case invite:
			i := inviteParts(in)

			switch {
			case i <= rnd:
			case i > rnd:
				rnd = i

				reply := newRsvp(i, vrnd, vval)
				outs.Put(reply)
			}
		case nominate:
			i, v := nominateParts(in)

			// SUPER IMPT MAD PAXOS
			if i < rnd || i == vrnd {
				continue
			}

			rnd = i
			vrnd = i
			vval = v

			broadcast := newVote(i, vval)
			outs.Put(broadcast)
		}
	}
}
