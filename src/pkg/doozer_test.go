package doozer

import (
	"doozer/client"
	"doozer/store"
	"github.com/bmizerany/assert"
	"net"
	"runtime"
	"sort"
	"testing"
)


// Upper bound on number of leaked goroutines.
// Our goal is to reduce this to zero.
const leaked = 23


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


func TestDoozerNoop(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl, err := client.Dial(l.Addr().String())
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, cl.Noop())
}


func TestDoozerGet(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl, err := client.Dial(l.Addr().String())
	assert.Equal(t, nil, err)

	ents, cas, err := cl.Get("/ping", 0)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, store.Dir, cas)
	assert.Equal(t, []string{"pong"}, ents)

	cl.Set("/test/a", "1", store.Missing)
	cl.Set("/test/b", "2", store.Missing)
	cl.Set("/test/c", "3", store.Missing)

	ents, cas, err = cl.Get("/test", 0)
	sort.SortStrings(ents)
	assert.Equal(t, store.Dir, cas)
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{"a", "b", "c"}, ents)
}


func TestDoozerWatchSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl, err := client.Dial(l.Addr().String())
	assert.Equal(t, nil, err)

	ch, err := cl.Watch("/test/**")
	assert.Equal(t, nil, err, err)
	defer close(ch)

	cl.Set("/test/foo", "bar", "")
	ev := <-ch
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, "bar", ev.Body)
	assert.NotEqual(t, "", ev.Cas)

	cl.Set("/test/fun", "house", "")
	ev = <-ch
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, "house", ev.Body)
	assert.NotEqual(t, "", ev.Cas)
}


func TestDoozerGoroutines(t *testing.T) {
	gs := runtime.Goroutines()

	func() {
		l := mustListen()
		defer l.Close()
		u := mustListenPacket(l.Addr().String())
		defer u.Close()

		go Main("a", "", u, l, nil)

		cl, err := client.Dial(l.Addr().String())
		assert.Equal(t, nil, err)
		cl.Noop()
	}()

	assert.T(t, gs+leaked >= runtime.Goroutines(), gs+leaked)
}


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

	cl, err := client.Dial(l.Addr().String())
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cl.Set("/test", "", store.Clobber)
	}
}
