package doozer

import (
	"doozer/client"
	"doozer/store"
	"testing"
)


func Benchmark1DoozerClientSet(b *testing.B) {
	b.StopTimer()
	l := mustListen()
	defer l.Close()
	a := l.Addr().String()
	u := mustListenPacket(a)
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := client.New("foo", l.Addr().String())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cl.Set("/test", store.Clobber, nil)
	}
}


func Benchmark1DoozerConClientSet(b *testing.B) {
	b.StopTimer()
	l := mustListen()
	defer l.Close()
	a := l.Addr().String()
	u := mustListenPacket(a)
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := client.New("foo", l.Addr().String())

	c := make(chan bool, b.N)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			cl.Set("/test", store.Clobber, nil)
			c <- true
		}()
	}
	for i := 0; i < b.N; i++ {
		<-c
	}
}


func Benchmark5DoozerClientSet(b *testing.B) {
	b.StopTimer()
	l := mustListen()
	defer l.Close()
	a := l.Addr().String()
	u := mustListenPacket(a)
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)

	cl := client.New("foo", l.Addr().String())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cl.Set("/test", store.Clobber, nil)
	}
}


func Benchmark5DoozerConClientSet(b *testing.B) {
	b.StopTimer()
	l := mustListen()
	defer l.Close()
	a := l.Addr().String()
	u := mustListenPacket(a)
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)
	go Main("a", a, mustListenPacket(":0"), mustListen(), nil, 1e9, 2e9, 3e9)

	cl := client.New("foo", l.Addr().String())

	c := make(chan bool, b.N)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			cl.Set("/test", store.Clobber, nil)
			c <- true
		}()
	}
	for i := 0; i < b.N; i++ {
		<-c
	}
}
