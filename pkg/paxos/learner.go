package paxos

type learner struct {
	round  uint64
	quorum uint64
	votes  map[string]uint64 // maps values to number of votes
	voted  map[int]bool      // maps nodes to vote status

	v    string
	done bool
}

func newLearner(quorum uint64) *learner {
	return &learner{
		round:  1,
		votes:  make(map[string]uint64),
		voted:  make(map[int]bool),
		quorum: quorum,
	}
}

func (ln *learner) Put(in Msg) {
	if in.Cmd() != vote {
		return
	}

	mRound, v := voteParts(in)

	switch {
	case mRound < ln.round:
		return
	case mRound > ln.round:
		ln.round = mRound
		ln.votes = make(map[string]uint64)
		ln.voted = make(map[int]bool)
		fallthrough
	case mRound == ln.round:
		if ln.voted[in.From()] {
			return
		}
		ln.votes[v]++
		ln.voted[in.From()] = true

		if ln.votes[v] >= ln.quorum {
			ln.done, ln.v = true, v // winner!
		}
	}
}
