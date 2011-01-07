package doozer

import (
	"doozer/client"
	"doozer/store"
	"github.com/bmizerany/assert"
	"net"
	//"sort"
	"strconv"
	"testing"
)


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

	cl := client.New("foo", l.Addr().String())
	err := cl.Noop()
	assert.Equal(t, nil, err)
}


func TestDoozerGet(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	ents, cas, err := cl.Get("/ping", 0)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, store.Dir, cas)
	assert.Equal(t, []byte("pong"), ents)

	//cl.Set("/test/a", store.Missing, []byte{'1'})
	//cl.Set("/test/b", store.Missing, []byte{'2'})
	//cl.Set("/test/c", store.Missing, []byte{'3'})

	//ents, cas, err = cl.Get("/test", 0)
	//sort.SortStrings(ents)
	//assert.Equal(t, store.Dir, cas)
	//assert.Equal(t, nil, err)
	//assert.Equal(t, []string{"a", "b", "c"}, ents)
}


func TestDoozerSnap(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	cas1, err := cl.Set("/x", store.Missing, []byte{'a'})
	assert.Equal(t, nil, err)
	ver1, err := strconv.Atoui64(cas1)
	assert.Equal(t, nil, err)

	sid, ver, err := cl.Snap()
	assert.Equal(t, nil, err)
	assert.Equal(t, int32(1), sid)
	assert.T(t, ver >= ver1)

	v, cas, err := cl.Get("/x", sid) // Use the snapshot.
	assert.Equal(t, nil, err)
	assert.Equal(t, cas1, cas)
	assert.Equal(t, []byte{'a'}, v)

	cas2, err := cl.Set("/x", cas, []byte{'b'})
	assert.Equal(t, nil, err)

	v, cas, err = cl.Get("/x", 0) // Read the new value.
	assert.Equal(t, nil, err)
	assert.Equal(t, cas2, cas)
	assert.Equal(t, []byte{'b'}, v)

	v, cas, err = cl.Get("/x", sid) // Read the saved value again.
	assert.Equal(t, nil, err)
	assert.Equal(t, cas1, cas)
	assert.Equal(t, []byte{'a'}, v)

	err = cl.DelSnap(sid)
	assert.Equal(t, nil, err)

	v, cas, err = cl.Get("/x", sid) // Use the missing snapshot.
	assert.Equal(t, client.ErrInvalidSnap, err)
	assert.Equal(t, "", cas)
	assert.Equal(t, []byte{}, v)
}


func TestDoozerWatchSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	ch, err := cl.Watch("/test/**")
	assert.Equal(t, nil, err, err)
	defer close(ch)

	cl.Set("/test/foo", "", []byte("bar"))
	ev := <-ch
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, []byte("bar"), ev.Body)
	assert.NotEqual(t, "", ev.Cas)

	cl.Set("/test/fun", "", []byte("house"))
	ev = <-ch
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, []byte("house"), ev.Body)
	assert.NotEqual(t, "", ev.Cas)
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

	cl := client.New("foo", l.Addr().String())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cl.Set("/test", store.Clobber, nil)
	}
}
