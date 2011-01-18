package paxos

type acceptor struct {
	outs      Putter
	rnd, vrnd int64
	vval      string
}

func (ac *acceptor) Put(m *M) {
	switch m.Cmd() {
	case M_INVITE:
		i := *m.Crnd

		if i > ac.rnd {
			ac.rnd = i

			ac.outs.Put(&M{
				WireCmd: rsvp,
				Crnd:    &i,
				Vrnd:    &ac.vrnd,
				Value:   []byte(ac.vval),
			})
		}
	case M_NOMINATE:
		i, v := *m.Crnd, m.Value

		// SUPER IMPT MAD PAXOS
		if i >= ac.rnd && i != ac.vrnd {
			ac.rnd = i
			ac.vrnd = i
			ac.vval = string(v)

			broadcast := &M{
				WireCmd: vote,
				Vrnd:    &i,
				Value:   []byte(ac.vval),
			}
			ac.outs.Put(broadcast)
		}
	}
}
