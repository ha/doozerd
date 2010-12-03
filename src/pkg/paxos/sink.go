package paxos

type sink struct {
	v    string
	done bool
}

func (s *sink) Put(m Msg) {
	switch m.Cmd() {
	case learn:
		s.done, s.v = true, learnParts(m)
	}
}
