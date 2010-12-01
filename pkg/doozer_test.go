package doozer

import (
	"doozer/client"
	"fmt"
	"net"
	"rand"
	"testing"
	"time"
)

// TODO make sure all these goroutines are cleaned up nicely

func randN() int32 {
	return rand.Int31n(252) + 1
}

func randAddr() string {
	port := rand.Int31n(63000) + 2000
	return fmt.Sprintf("127.%d.%d.%d:%d", randN(), randN(), randN(), port)
}

func mustListen() net.Listener {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	return l
}

func TestFoo(t *testing.T) {
	l := mustListen()
	a0, w := l.Addr().String(), randAddr()
	fmt.Println("web", w)
	go Main("a", "", w, l)
	time.Sleep(1e8)
	go Main("a", a0, "", mustListen())
	go Main("a", a0, "", mustListen())
	time.Sleep(1e8)

	cl, err := client.Dial(a0)
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Noop()
	if err != nil {
		t.Fatal(err)
	}

	// cl.Get("/doozer/members")
	//for m in members {
	//	cl.Get(/session/m)
	//	cl.Get(/doozer/info/m/applied)
	//}
}
