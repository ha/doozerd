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

func (ln *learner) Deliver(p Packet) {
	in := p.M
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
		ln.voted = make(map[string]bool)
		fallthrough
	case mRound == ln.round:
		k := string(v)

		if ln.voted[p.Addr] {
			return
		}
		ln.votes[k]++
		ln.voted[p.Addr] = true

		if ln.votes[k] >= ln.quorum {
			ln.done, ln.v = true, string(v) // winner!
		}
	}
	return
}
