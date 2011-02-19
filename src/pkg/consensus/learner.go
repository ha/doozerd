package consensus

type learner struct {
	round  int64
	quorum int64
	votes  map[string]int64 // maps values to number of votes
	voted  map[string]bool  // maps nodes to vote status

	v    string
	done bool
}

func (ln *learner) init(quorum int64) {
	ln.round = 1
	ln.votes = make(map[string]int64)
	ln.voted = make(map[string]bool)
	ln.quorum = quorum
}

func (ln *learner) deliver(p packet) (m *M, v []byte, ok bool) {
	if ln.done {
		return
	}

	in := p.M
	if in.Cmd == nil {
		return
	}

	switch *in.Cmd {
	case M_LEARN:
		ln.done, ln.v = true, string(in.Value)
		return nil, in.Value, true
	case M_VOTE:
		if in.Vrnd == nil {
			break
		}

		mRound, v := *in.Vrnd, in.Value

		switch {
		case mRound < ln.round:
			break
		case mRound > ln.round:
			ln.round = mRound
			ln.votes = make(map[string]int64)
			ln.voted = make(map[string]bool)
			fallthrough
		case mRound == ln.round:
			k := string(v)

			if ln.voted[p.Addr] {
				break
			}
			ln.votes[k]++
			ln.voted[p.Addr] = true

			if ln.votes[k] >= ln.quorum {
				// winner!
				ln.done, ln.v = true, string(v)
				return &M{Cmd: learn, Value: v}, v, true
			}
		}
	}
	return
}
