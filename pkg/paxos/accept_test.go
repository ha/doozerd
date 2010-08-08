package paxos

import (
    "fmt"
    "strings"
    "strconv"

    "borg/assert"
    "testing"
    "container/vector"
)

const (
    iFrom = iota
    iTo
    iCmd
    iBody
    iNumParts
)

const (
    nRnd = iota
    nVal
    nNumParts
)

func accept(me uint64, ins, outs chan string) {
    var rnd, vrnd uint64
    var vval string

    ch, sent := make(chan int), 0
    for in := range ins {
        parts := strings.Split(in, ":", iNumParts)
        if len(parts) != iNumParts {
            continue
        }

        inTo, _ := strconv.Btoui64(parts[iTo], 10)
        if inTo != me && parts[iTo] != "*" {
            continue
        }


        switch parts[iCmd] {
        case "INVITE":
            i, err := strconv.Btoui64(parts[iBody], 10)
            if err != nil { continue }

            inFrom, err := strconv.Btoui64(parts[iFrom], 10)
            if err != nil { continue }


            switch {
                case i <= rnd:
                case i > rnd:
                    rnd = i

                    outTo := inFrom // reply to the sender
                    msg := fmt.Sprintf("%d:%d:ACCEPT:%d:%d:%s",
                        me,
                        outTo,
                        i,
                        vrnd,
                        vval,
                    )
                    go func(msg string) { outs <- msg ; ch <- 1 }(msg)
                    sent++
            }
        case "NOMINATE":
            nominateParts := strings.Split(parts[iBody], ":", nNumParts)
            if len(nominateParts) != nNumParts {
                continue
            }

            i, err := strconv.Btoui64(nominateParts[nRnd], 10)
            if err != nil {
                continue
            }

            if i < rnd {
                continue
            }

            val := nominateParts[nVal]
            rnd = i

            msg := fmt.Sprintf("%d:*:VOTE:%d:%s", me, i, val)
            go func(msg string) { outs <- msg ; ch <- 1 }(msg)
            sent++
        }
    }

    for x := 0; x < sent; x++ {
        <-ch
    }

    close(outs)
}



// TESTING

func gather(ch chan string) (got []string) {
    for x := range ch { (*vector.StringVector)(&got).Push(x) }
    return
}

func TestAcceptsInvite(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := []string{"2:1:ACCEPT:1:0:"}

    go accept(2, ins, outs)
    ins <- "1:*:INVITE:1"
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}

func TestInvitesAfterNewInvitesAreStaleAndIgnored(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := []string{"2:1:ACCEPT:2:0:"}

    go accept(2, ins, outs)
    ins <- "1:*:INVITE:2"
    ins <- "1:*:INVITE:1"
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}

func TestIgnoresMalformedMessages(t *testing.T) {
    totest := []string{
        "x", // too few separators
        "x:x", // too few separators
        "x:x:x", // too few separators
        "x:x:x:x:x", // too many separators
        "1:*:INVITE:x", // invalid round number
        "1:*:x:1", // unknown command
        "1:x:INVITE:1", // invalid to address
        "1:7:INVITE:1", // valid but incorrect to address
        "X:*:INVITE:1", // invalid from address

        "1:*:NOMINATE:x", // too few separators in nominate body
        "1:*:NOMINATE:x:foo", // invalid round number
    }

    for _, msg := range(totest) {
        ins := make(chan string)
        outs := make(chan string)

        exp := []string{}

        go accept(2, ins, outs)
        ins <- msg
        close(ins)

        // outs was closed; therefore all messages have been processed
        assert.Equal(t, exp, gather(outs), "")
    }
}

func TestItVotes(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    val := "foo"

    exp := []string{"2:*:VOTE:1:" + val}

    go accept(2, ins, outs)
    // According to paxos, we can omit Phase 1 in round 1
    ins <- "1:*:NOMINATE:1:" + val
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}

func TestItVotesWithAnotherValue(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    val := "bar"

    exp := []string{"2:*:VOTE:1:" + val}

    go accept(2, ins, outs)
    // According to paxos, we can omit Phase 1 in round 1
    ins <- "1:*:NOMINATE:1:" + val
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}

func TestItVotesWithAnotherRound(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    val := "bar"

    exp := []string{"2:*:VOTE:2:" + val}

    go accept(2, ins, outs)
    // According to paxos, we can omit Phase 1 in the first round
    ins <- "1:*:NOMINATE:2:" + val
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}

func TestItVotesWithAnotherSelf(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    val := "bar"

    exp := []string{"3:*:VOTE:2:" + val}

    go accept(3, ins, outs)
    // According to paxos, we can omit Phase 1 in the first round
    ins <- "1:*:NOMINATE:2:" + val
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}

func TestItIgnoresOldNominations(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    val := "bar"

    exp := []string{}

    go accept(3, ins, outs)
    // According to paxos, we can omit Phase 1 in the first round
    ins <- "1:*:INVITE:2"
    <-outs // throw away ACCEPT message
    ins <- "1:*:NOMINATE:1:" + val
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}

func TestInvitesAfterNewNominationsAreStaleAndIgnored(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := []string{}

    go accept(2, ins, outs)
    ins <- "1:*:NOMINATE:2:v"
    <-outs // throw away VOTE message
    ins <- "1:*:INVITE:1"
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, gather(outs), "")
}
