package paxos

func learner(quorum uint64, ins chan Msg) string {
	round := uint64(1)
	votes := make(map[string]uint64) // maps values to number of votes
	voted := make(map[int]bool)      // maps nodes to vote status

	for in := range ins {
		if in.Cmd() != vote {
			continue
		}

		mRound, v := voteParts(in)

		switch {
		case mRound < round:
			continue
		case mRound > round:
			round = mRound
			votes = make(map[string]uint64)
			voted = make(map[int]bool)
			fallthrough
		case mRound == round:
			if voted[in.From()] {
				continue
			}
			votes[v]++
			voted[in.From()] = true

			if votes[v] >= quorum {
				return v // winner!
			}
		}
	}
	return ""
}
