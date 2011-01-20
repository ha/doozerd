package paxos

type sink struct {
	v    string
	done bool
}

func (s *sink) Put(m *M) {
	if s.done {
		return
	}

	switch m.Cmd() {
	case M_LEARN:
		s.done, s.v = true, string(m.Value)
	}
	return
}
