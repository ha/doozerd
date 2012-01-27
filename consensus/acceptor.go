package consensus

type acceptor struct {
	rnd, vrnd int64
	vval      string
}

func (ac *acceptor) update(m *msg) *msg {
	switch *m.Cmd {
	case msg_INVITE:
		if m.Crnd == nil {
			break
		}

		i := *m.Crnd

		if i > ac.rnd {
			ac.rnd = i

			return &msg{
				Cmd:   rsvp,
				Crnd:  &i,
				Vrnd:  &ac.vrnd,
				Value: []byte(ac.vval),
			}
		}
	case msg_NOMINATE:
		if m.Crnd == nil {
			break
		}

		i, v := *m.Crnd, m.Value

		// SUPER IMPT MAD PAXOS
		if i >= ac.rnd && i != ac.vrnd {
			ac.rnd = i
			ac.vrnd = i
			ac.vval = string(v)

			broadcast := &msg{
				Cmd:   vote,
				Vrnd:  &i,
				Value: []byte(ac.vval),
			}
			return broadcast
		}
	}
	return nil
}
