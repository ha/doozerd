package paxos

import (
    "fmt"
    "strings"
    "strconv"

    "borg/assert"
    "testing"
)

const (
    iSender = iota
    iCmd
    iRnd
    iNumParts
)

func accept(quorum int, ins, outs chan string) {
    var rnd, vrnd uint64
    var vval string

    ch, sent := make(chan int), 0
    for in := range ins {
        parts := strings.Split(in, ":", 3)
        if len(parts) != iNumParts {
            continue
        }
        i, _ := strconv.Btoui64(parts[iRnd], 10)
        // If parts[iRnd] is invalid, i is 0 and the message will be ignored
        switch {
            case i <= rnd:
            case i > rnd:
                rnd = i

                sent++
                msg := fmt.Sprintf("ACCEPT:%d:%d:%s", i, vrnd, vval)
                go func(msg string) { outs <- msg ; ch <- 1 }(msg)
        }
    }

    for x := 0; x < sent; x++ {
        <-ch
    }

    close(outs)
}

func TestAcceptsInvite(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := "ACCEPT:1:0:"

    go accept(2, ins, outs)
    // Send a message with no senderId
    ins <- "1:INVITE:1"
    close(ins)

    got := ""
    for x := range outs {
        got += x
    }

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, got, "")
}

func TestIgnoresStaleInvites(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := "ACCEPT:2:0:"

    go accept(2, ins, outs)
    // Send a message with no senderId
    ins <- "1:INVITE:2"
    ins <- "1:INVITE:1"
    close(ins)

    got := ""
    for x := range outs {
        got += x
    }

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, got, "")
}

func TestIgnoresMalformedMessageWithTooFewSeparators(t *testing.T) {
    totest := []string{
        "x",
        "x:x",
        "x:x:x:x",
    }
    for _, msg := range(totest) {
        ins := make(chan string)
        outs := make(chan string)

        exp := ""

        go accept(2, ins, outs)
        // Send a message with no senderId
        ins <- msg
        close(ins)

        got := ""
        for x := range outs {
            got += x
        }

        // outs was closed; therefore all messages have been processed
        assert.Equal(t, exp, got, "")
    }
}

func TestIgnoresMalformedMessageWithInvalidRound(t *testing.T) {
    ins := make(chan string)
    outs := make(chan string)

    exp := ""

    go accept(2, ins, outs)
    // Send a message with no senderId
    ins <- "1:INVITE:x"
    close(ins)

    got := ""
    for x := range outs {
        got += x
    }

    // outs was closed; therefore all messages have been processed
    assert.Equal(t, exp, got, "")
}

