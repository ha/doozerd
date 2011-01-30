package doozer

import (
	"doozer/client"
	"doozer/store"
	"testing"
)


func BenchmarkDoozerClientSet(b *testing.B) {
	b.StopTimer()
	l := mustListen()
	defer l.Close()
	a := l.Addr().String()
	u := mustListenPacket(a)
	defer u.Close()

	go Main("a", "", u, l, nil)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil)

	cl := client.New("foo", l.Addr().String())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cl.Set("/test", store.Clobber, nil)
	}
}
