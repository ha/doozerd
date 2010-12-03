package paxos

type acceptor struct {
	outs      Putter
	rnd, vrnd uint64
	vval      string
}

func (ac *acceptor) Put(m Msg) {
	switch m.Cmd() {
	case invite:
		if i := inviteParts(m); i > ac.rnd {
			ac.rnd = i

			reply := newRsvp(i, ac.vrnd, ac.vval)
			ac.outs.Put(reply)
		}
	case nominate:
		i, v := nominateParts(m)

		// SUPER IMPT MAD PAXOS
		if i >= ac.rnd && i != ac.vrnd {
			ac.rnd = i
			ac.vrnd = i
			ac.vval = v

			broadcast := newVote(i, ac.vval)
			ac.outs.Put(broadcast)
		}
	}
}
