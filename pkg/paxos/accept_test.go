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
    iRnd
    iNumParts
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
            i, err := strconv.Btoui64(parts[iRnd], 10)
            if err != nil { continue }

            inFrom, err := strconv.Btoui64(parts[iFrom], 10)
            if err != nil { continue }

            switch {
                case i <= rnd:
                case i > rnd:
                    rnd = i

                    sent++
                    outTo := inFrom // reply to the sender
                    msg := fmt.Sprintf(
                        "%d:%d:ACCEPT:%d:%d:%s",
                        me,
                        outTo,
                        i,
                        vrnd,
                        vval,
                    )
                    go func(msg string) { outs <- msg ; ch <- 1 }(msg)
            }
        }
    }

    for x := 0; x < sent; x++ {
        <-ch
    }

    close(outs)
}



// TESTING

func slurp(ch chan string) (got []string) {
    for x := range ch { (*vector.StringVector)(&got).Push(x) }
    return
}

func TestAcceptsInvite(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := []string{"2:1:ACCEPT:1:0:"}

    go accept(2, ins, outs)
    // Send a message with no senderId
    ins <- "1:*:INVITE:1"
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, slurp(outs), "")
}

func TestIgnoresStaleInvites(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := []string{"2:1:ACCEPT:2:0:"}

    go accept(2, ins, outs)
    // Send a message with no senderId
    ins <- "1:*:INVITE:2"
    ins <- "1:*:INVITE:1"
    close(ins)

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, slurp(outs), "")
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
    }

    for _, msg := range(totest) {
        ins := make(chan string)
        outs := make(chan string)

        exp := []string{}

        go accept(2, ins, outs)
        // Send a message with no senderId
        ins <- msg
        close(ins)

        // outs was closed; therefore all messages have been processed
        assert.Equal(t, exp, slurp(outs), "")
    }
}
