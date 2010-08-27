package paxos

func learner(quorum uint64, ins chan Message) string {
	var round uint64 = 1
	votes := make(map[string]uint64) // maps values to number of votes
	voted := make(map[int]bool)      // maps nodes to vote status

	for in := range ins {
		if in.Cmd() != Vote {
			continue
		}

		mRound, v := VoteParts(in)

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
