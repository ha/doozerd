package paxos

func sink(ch chan Msg) string {
	for m := range ch {
		switch m.Cmd() {
		case learn:
			return learnParts(m)
		}
	}
	return ""
}
