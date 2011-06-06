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

	go Main("a", "X", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

	cl := dial(l.Addr().String())

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

	go Main("a", "X", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

	cl := dial(l.Addr().String())

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

	go Main("a", "X", "", "", "", nil, u, l, nil, 1e9, 1e8, 3e9, 101)
	go Main("a", "Y", "", "", "", dial(a), u1, l1, nil, 1e9, 1e8, 3e9, 101)
	go Main("a", "Z", "", "", "", dial(a), u2, l2, nil, 1e9, 1e8, 3e9, 101)
	go Main("a", "V", "", "", "", dial(a), u3, l3, nil, 1e9, 1e8, 3e9, 101)
	go Main("a", "W", "", "", "", dial(a), u4, l4, nil, 1e9, 1e8, 3e9, 101)

	cl := dial(l.Addr().String())
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
	if b.N < 5 {
		return
	}
	const C = 20
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

	go Main("a", "X", "", "", "", nil, u, l, nil, 1e9, 1e10, 3e12, 1e9)
	go Main("a", "Y", "", "", "", dial(a), u1, l1, nil, 1e9, 1e10, 3e12, 1e9)
	go Main("a", "Z", "", "", "", dial(a), u2, l2, nil, 1e9, 1e10, 3e12, 1e9)
	go Main("a", "V", "", "", "", dial(a), u3, l3, nil, 1e9, 1e10, 3e12, 1e9)
	go Main("a", "W", "", "", "", dial(a), u4, l4, nil, 1e9, 1e10, 3e12, 1e9)

	cl := dial(l.Addr().String())
	cl.Set("/ctl/cal/1", store.Missing, nil)
	cl.Set("/ctl/cal/2", store.Missing, nil)
	cl.Set("/ctl/cal/3", store.Missing, nil)
	cl.Set("/ctl/cal/4", store.Missing, nil)

	waitFor(cl, "/ctl/node/X/writable")
	waitFor(cl, "/ctl/node/Y/writable")
	waitFor(cl, "/ctl/node/Z/writable")
	waitFor(cl, "/ctl/node/V/writable")
	waitFor(cl, "/ctl/node/W/writable")

	cls := []*doozer.Conn{
		cl,
		dial(l1.Addr().String()),
		dial(l2.Addr().String()),
		dial(l3.Addr().String()),
		dial(l4.Addr().String()),
	}

	done := make(chan bool, C)
	f := func(i int, cl *doozer.Conn) {
		for ; i < b.N; i += C {
			_, err := cl.Set("/test", store.Clobber, nil)
			if e, ok := err.(*doozer.Error); ok && e.Err == doozer.ErrReadonly {
			} else if err != nil {
				panic(err)
			}
		}
		done <- true
	}
	b.StartTimer()
	for i := 0; i < C; i++ {
		go f(i, cls[i%len(cls)])
	}
	for i := 0; i < C; i++ {
		<-done
	}
}
