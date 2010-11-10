package net

import (
	"container/heap"
	"container/vector"
	"junta/paxos"
	"junta/util"
	"math"
	"net"
	"os"
	"time"
)

const (
	interval = 1e8  // ns == 100ms
	timeout  = 1e10 // ns == 10s

	max = 3000 // bytes. Definitely big enough for UDP over Ethernet.
)

var logger = util.NewLogger("net")

type Conn interface {
	ReadFrom([]byte) (int, net.Addr, os.Error)
	WriteTo([]byte, net.Addr) (int, os.Error)
	LocalAddr() net.Addr
}

type check struct {
	paxos.Packet
	at, until int64
}

func (k check) Less(y interface{}) bool {
	return k.at < y.(check).at
}

func Ackify(c Conn, w <-chan paxos.Packet) (r <-chan paxos.Packet) {
	in := make(chan paxos.Packet)
	go process(c, in, w)
	return in
}

func process(c Conn, in chan paxos.Packet, out <-chan paxos.Packet) {
	pend := make(map[string]bool)
	rawIn := make(chan paxos.Packet)
	ticker := time.Tick(interval / 4)
	h := new(vector.Vector)

	go recv(c, rawIn)

	peek := func() check {
		if h.Len() < 1 {
			return check{at: math.MaxInt64}
		}
		return h.At(0).(check)
	}

	for {
		select {
		case p := <-rawIn:
			if p.Msg.HasFlags(paxos.Ack) {
				if pend[p.Id()] {
					pend[p.Id()] = false, false
				}
				continue
			}

			in <- p

			// send ack
			write(c, p.Msg.Dup().SetFlags(paxos.Ack), p.Addr)
		case p := <-out:
			pend[p.Id()] = true
			write(c, p.Msg, p.Addr)
			t := time.Nanoseconds()
			heap.Push(h, check{p, t + interval, t + timeout})
		case t := <-ticker:
			for k := peek(); k.at < t; k = peek() {
				heap.Pop(h)
				if t > k.until {
					pend[k.Id()] = false, false
				}
				if pend[k.Id()] {
					write(c, k.Msg, k.Addr)
					heap.Push(h, check{k.Packet, t + interval, k.until})
				}
			}
		}
	}
}

func recv(c Conn, rawIn chan paxos.Packet) {
	for {
		msg, addr, err := paxos.ReadMsg(c, max)
		if err != nil {
			logger.Println(err)
			continue
		}
		rawIn <- paxos.Packet{msg, addr}
	}
}

func write(c Conn, m paxos.Msg, a string) os.Error {
	addr, err := net.ResolveUDPAddr(a)
	if err != nil {
		return err
	}

	_, err = c.WriteTo(m.WireBytes(), addr)
	return err
}
