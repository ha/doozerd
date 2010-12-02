package doozer

import (
	"doozer/client"
	"github.com/bmizerany/assert"
	"net"
	"testing"
)

// import "runtime"

func mustListen() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	return l
}

func mustListenPacket(addr string) net.PacketConn {
	c, err := net.ListenPacket("udp", addr)
	if err != nil {
		panic(err)
	}
	return c
}

func TestDoozerSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl, err := client.Dial(l.Addr().String())
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, cl.Noop())
}

// func TestWatch(t *testing.T) {
// 	for { runtime.Gosched() }
// }
