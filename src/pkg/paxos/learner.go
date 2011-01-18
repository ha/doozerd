package paxos

type learner struct {
	round  int64
	quorum int64
	votes  map[string]int64 // maps values to number of votes
	voted  map[int]bool     // maps nodes to vote status

	v    string
	done bool
}

func newLearner(quorum int64) *learner {
	return &learner{
		round:  1,
		votes:  make(map[string]int64),
		voted:  make(map[int]bool),
		quorum: quorum,
	}
}

func (ln *learner) Put(in *M) (ok bool) {
	if in.Cmd() != M_VOTE {
		return
	}

	mRound, v := *in.Vrnd, in.Value

	switch {
	case mRound < ln.round:
		return
	case mRound > ln.round:
		ln.round = mRound
		ln.votes = make(map[string]int64)
		ln.voted = make(map[int]bool)
		fallthrough
	case mRound == ln.round:
		k := string(v)

		if ln.voted[in.From()] {
			return
		}
		ln.votes[k]++
		ln.voted[in.From()] = true

		if ln.votes[k] >= ln.quorum {
			ok, ln.done, ln.v = true, true, string(v) // winner!
		}
	}
	return
}
