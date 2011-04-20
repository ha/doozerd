package peer

import (
	"doozer/store"
	"github.com/ha/doozer"
	"testing"
)


func Benchmark1DoozerClientSet(b *testing.B) {
	b.StopTimer()
	l := mustListen()
	defer l.Close()
	a := l.Addr().String()
	u := mustListenPacket(a)
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

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

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

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

	l1 := mustListen()
	defer l1.Close()
	u1 := mustListenPacket(l1.Addr().String())
	defer u1.Close()
	l2 := mustListen()
	defer l2.Close()
	u2 := mustListenPacket(l2.Addr().String())
	defer u2.Close()
	l3 := mustListen()
	defer l3.Close()
	u3 := mustListenPacket(l3.Addr().String())
	defer u3.Close()
	l4 := mustListen()
	defer l4.Close()
	u4 := mustListenPacket(l4.Addr().String())
	defer u4.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 1e8, 3e9)
	go Main("a", "Y", "", doozer.New("a", a), u1, l1, nil, 1e9, 1e8, 3e9)
	go Main("a", "Z", "", doozer.New("a", a), u2, l2, nil, 1e9, 1e8, 3e9)
	go Main("a", "V", "", doozer.New("a", a), u3, l3, nil, 1e9, 1e8, 3e9)
	go Main("a", "W", "", doozer.New("a", a), u4, l4, nil, 1e9, 1e8, 3e9)

	cl := doozer.New("foo", l.Addr().String())
	cl.Set("/ctl/cal/1", store.Missing, nil)
	cl.Set("/ctl/cal/2", store.Missing, nil)
	cl.Set("/ctl/cal/3", store.Missing, nil)
	cl.Set("/ctl/cal/4", store.Missing, nil)

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

	l1 := mustListen()
	defer l1.Close()
	u1 := mustListenPacket(l1.Addr().String())
	defer u1.Close()
	l2 := mustListen()
	defer l2.Close()
	u2 := mustListenPacket(l2.Addr().String())
	defer u2.Close()
	l3 := mustListen()
	defer l3.Close()
	u3 := mustListenPacket(l3.Addr().String())
	defer u3.Close()
	l4 := mustListen()
	defer l4.Close()
	u4 := mustListenPacket(l4.Addr().String())
	defer u4.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 1e8, 3e9)
	go Main("a", "Y", "", doozer.New("a", a), u1, l1, nil, 1e9, 1e8, 3e9)
	go Main("a", "Z", "", doozer.New("a", a), u2, l2, nil, 1e9, 1e8, 3e9)
	go Main("a", "V", "", doozer.New("a", a), u3, l3, nil, 1e9, 1e8, 3e9)
	go Main("a", "W", "", doozer.New("a", a), u4, l4, nil, 1e9, 1e8, 3e9)

	cl := doozer.New("foo", l.Addr().String())
	cl.Set("/ctl/cal/1", store.Missing, nil)
	cl.Set("/ctl/cal/2", store.Missing, nil)
	cl.Set("/ctl/cal/3", store.Missing, nil)
	cl.Set("/ctl/cal/4", store.Missing, nil)

	cls := []*doozer.Client{
		cl,
		doozer.New("foo", l1.Addr().String()),
		doozer.New("foo", l2.Addr().String()),
		doozer.New("foo", l3.Addr().String()),
		doozer.New("foo", l4.Addr().String()),
	}

	c := make(chan bool, b.N)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		i := i
		go func() {
			cls[i%len(cls)].Set("/test", store.Clobber, nil)
			c <- true
		}()
	}
	for i := 0; i < b.N; i++ {
		<-c
	}
}
