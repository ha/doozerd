package paxos

import (
    "strconv"
)

const (
    lRnd = iota
    lValue
    lNumParts
)

func learn(quorum int, ins chan msg, taught chan string, ack func()) {
    var round uint64 = 0
    votes := make(map[string]int) // maps values to number of votes
    voted := make(map[uint64]bool) // maps values to number of votes

    for in := range ins {
        parts, err := splitBody(in.body, lNumParts) // e.g. VOTE:1:xxx
        if err != nil {
            continue
        }

        if len(parts) != lNumParts {
            continue
        }

        if in.cmd != "VOTE" {
            continue
        }

        mRound, err := strconv.Btoui64(parts[lRnd], 10)
        if err != nil {
            continue
        }

        v := parts[lValue]

        ack()

        switch {
        case mRound < round:
            continue
        case mRound > round:
            round = mRound
            votes = make(map[string]int)
            voted = make(map[uint64]bool)
            fallthrough
        case mRound == round:
            if voted[in.from] {
                continue
            }
            votes[v]++
            voted[in.from] = true

            if votes[v] >= quorum {
                taught <- v // winner!
                return
            }
        }

    }
}
