package peer

import (
	_ "doozer/quiet"
	"github.com/nightmouse/doozer"
	"net"
)

func mustListen() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	return l
}


func mustListenUDP(addr string) *net.UDPConn {
	uaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		panic(err)
	}
	c, err := net.ListenUDP("udp", uaddr)
	if err != nil {
		panic(err)
	}
	return c
}


func dial(addr string) *doozer.Conn {
	c, err := doozer.Dial(addr)
	if err != nil {
		panic(err)
	}
	return c
}


func waitFor(cl *doozer.Conn, path string) {
	var rev int64
	for {
		ev, err := cl.Wait(path, rev)
		if err != nil {
			panic(err)
		}
		if ev.IsSet() && len(ev.Body) > 0 {
			break
		}
		rev = ev.Rev + 1
	}
}
