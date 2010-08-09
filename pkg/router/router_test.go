package router

import (
    "borg/paxos"

    "borg/assert"
    "testing"
)

type cf func(me, nNodes uint64, target string, ins, outs chan paxos.Msg, clock chan int)
type af func(me uint64, ins, outs chan paxos.Msg)
type lf func(quorum int, ins chan paxos.Msg, taught chan string, ack func())

func router(propIns, packetIns, packetOuts chan string, c cf, a af, l lf) {
    target := <-propIns
    go c(1, 2, target, nil, nil, nil)
    go a(1, nil, nil)
    go l(1, nil, nil, nil)
}

func TestStartNewPaxos(t *testing.T) {
    val := "set:foo:bar"
    cTarget := make(chan string)
    aStarted := make(chan int)
    lStarted := make(chan int)
    propIns := make(chan string)

    c := func(me, nNodes uint64, target string, ins, outs chan paxos.Msg, clock chan int) {
        cTarget <- target
    }

    a := func(me uint64, ins, outs chan paxos.Msg) {
        aStarted <- 1
    }

    l := func(quorum int, ins chan paxos.Msg, taught chan string, ack func()) {
        lStarted <- 1
    }

    go router(propIns, nil, nil, c, a, l)

    propIns <- val

    assert.Equal(t, val, <-cTarget, "")
    assert.Equal(t, 1, <-aStarted, "")
    assert.Equal(t, 1, <-lStarted, "")
}

func TestSomethingElse(t *testing.T) {
    exp := "1:*:INVITE:1"
    cTarget := make(chan string)
    aStarted := make(chan int)
    lStarted := make(chan int)
    propIns := make(chan string)

    c := func(me, nNodes uint64, target string, ins, outs chan paxos.Msg, clock chan int) {
        cTarget <- target
        outs <- m(exp)
    }

    a := func(me uint64, ins, outs chan paxos.Msg) {
        aStarted <- 1
    }

    l := func(quorum int, ins chan paxos.Msg, taught chan string, ack func()) {
        lStarted <- 1
    }

    go router(propIns, nil, packetOuts, c, a, l)

    assert.Equal(t, "1--" + exp, packetOuts, "")
}

