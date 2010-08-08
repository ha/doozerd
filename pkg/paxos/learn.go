package paxos

import (
    "strconv"
    "strings"
)

const (
    lFrom = iota
    lTo
    lCmd
    lRnd
    lValue
    lNumParts
)

func learn(quorum int, messages, taught chan string, ack func()) {
    var round uint64 = 0
    votes := make(map[string]int) // maps values to number of votes
    voted := make(map[uint64]bool) // maps values to number of votes

    for m := range messages {
        parts := strings.Split(m, ":", lNumParts) // e.g. VOTE:1:xxx

        if parts[lCmd] != "VOTE" {
            continue
        }

        mRound, err := strconv.Btoui64(parts[lRnd], 10)
        if err != nil {
            continue
        }

        sender, err := strconv.Btoui64(parts[lFrom], 10)
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
            if voted[sender] {
                continue
            }
            votes[v]++
            voted[sender] = true

            if votes[v] >= quorum {
                taught <- v // winner!
                return
            }
        }

    }
}
