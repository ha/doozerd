package paxos

const (
    lRnd = iota
    lValue
    lNumParts
)

func learner(quorum int, ins chan msg, taught chan string, ack func()) {
    var round uint64 = 0
    votes := make(map[string]int) // maps values to number of votes
    voted := make(map[uint64]bool) // maps values to number of votes

    update := func(in msg) {
        defer swallowContinue()

        parts := splitExactly(in.body, lNumParts) // e.g. 1:xxx

        if in.cmd != "VOTE" {
            return
        }

        mRound := dtoui64(parts[lRnd])

        v := parts[lValue]

        ack()

        switch {
        case mRound < round:
            return
        case mRound > round:
            round = mRound
            votes = make(map[string]int)
            voted = make(map[uint64]bool)
            fallthrough
        case mRound == round:
            if voted[in.from] {
                return
            }
            votes[v]++
            voted[in.from] = true

            if votes[v] >= quorum {
                taught <- v // winner!
                return
            }
        }

    }

    for in := range ins {
        update(in)
    }
}
