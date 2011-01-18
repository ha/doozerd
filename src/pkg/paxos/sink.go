package paxos

type sink struct {
	v    string
	done bool
}

func (s *sink) Put(m *M) (ok bool) {
	if s.done {
		return
	}

	switch m.Cmd() {
	case M_LEARN:
		ok, s.done, s.v = true, true, string(m.Value)
	}
	return
}
