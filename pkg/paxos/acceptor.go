package paxos

func acceptor(ins chan Message, outs Putter) {
	var rnd, vrnd uint64
	var vval string

	for in := range ins {
		switch in.Cmd() {
		case Invite:
			i := InviteParts(in)

			switch {
			case i <= rnd:
			case i > rnd:
				rnd = i

				reply := NewRsvp(i, vrnd, vval)
				outs.Put(reply)
			}
		case Nominate:
			i, v := NominateParts(in)

			// SUPER IMPT MAD PAXOS
			if i < rnd || i == vrnd {
				continue
			}

			rnd = i
			vrnd = i
			vval = v

			broadcast := NewVote(i, vval)
			outs.Put(broadcast)
		}
	}
}
