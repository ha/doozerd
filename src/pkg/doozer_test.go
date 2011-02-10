package doozer

import (
	"doozer/client"
	"doozer/store"
	"exec"
	"github.com/bmizerany/assert"
	"net"
	"sort"
	"testing"
	"time"
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


func TestDoozerSet(t *testing.T) {
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

	for i := byte(0); i < 10; i++ {
		cl.Set("/x", store.Missing, []byte{'0'+i})
		assert.Equal(t, nil, err)
	}
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

	sid, ver, err := cl.Snap()
	assert.Equal(t, nil, err)
	assert.Equal(t, int32(1), sid)
	assert.T(t, ver >= cas1)

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
	assert.Equal(t, int64(0), cas)
	assert.Equal(t, []byte{}, v)
}


func TestDoozerWatchSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	w, err := cl.Watch("/test/**")
	assert.Equal(t, nil, err, err)
	defer w.Cancel()

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	ev := <-w.C
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, []byte("bar"), ev.Body)
	assert.NotEqual(t, "", ev.Cas)

	cl.Set("/test/fun", store.Clobber, []byte("house"))
	ev = <-w.C
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, []byte("house"), ev.Body)
	assert.NotEqual(t, "", ev.Cas)

	w.Cancel()
	ev = <-w.C
	assert.Tf(t, closed(w.C), "got %v", ev)
}


func TestDoozerWalk(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	cl.Set("/test/fun", store.Clobber, []byte("house"))

	w, err := cl.Walk("/test/**", 0)
	assert.Equal(t, nil, err, err)

	ev := <-w.C
	assert.NotEqual(t, (*client.Event)(nil), ev)
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, "bar", string(ev.Body))
	assert.NotEqual(t, "", ev.Cas)

	ev = <-w.C
	assert.NotEqual(t, (*client.Event)(nil), ev)
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, "house", string(ev.Body))
	assert.NotEqual(t, "", ev.Cas)

	ev = <-w.C
	assert.Tf(t, closed(w.C), "got %v", ev)
}

func TestDoozerStat(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	setCas, _ := cl.Set("/test/fun", store.Clobber, []byte("house"))

	ln, cas, err := cl.Stat("/test", 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, store.Dir, cas)
	assert.Equal(t, int32(2), ln)

	ln, cas, err = cl.Stat("/test/fun", 0)
	assert.Equal(t, nil, err)
	assert.Equal(t, setCas, cas)
	assert.Equal(t, int32(5), ln)
}

func TestDoozerGetDirOnDir(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))

	w, err := cl.GetDir("/test", 0, 0, 0)
	assert.Equal(t, nil, err)

	got := make([]string, 0)
	for e := range w.C {
		got = append(got, e.Path)
	}

	sort.SortStrings(got)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestDoozerGetDirOnFile(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("1"))

	w, err := cl.GetDir("/test/a", 0, 0, 0)
	assert.Equal(t, nil, err)

	exp := &client.ResponseError{Code:20, Detail:"not a directory"}
	e   := <-w.C
	assert.Equal(t, exp, e.Err)
}

func TestDoozerGetDirMissing(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())

	w, err := cl.GetDir("/not/here", 0, 0, 0)
	assert.Equal(t, nil, err)

	e   := <-w.C
	exp := &client.ResponseError{Code:22, Detail:"NOENT"}
	assert.Equal(t, exp, e.Err)
}

func TestDoozerGetDirOffsetLimit(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())
	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))
	cl.Set("/test/d", store.Clobber, []byte("4"))

	// The order is arbitrary.  We need to collect them
	// because it's not safe to assume the order.
	w, _ := cl.GetDir("/test", 0, 0, 0)
	ents := make([]string, 0)
	for e := range w.C {
		ents = append(ents, e.Path)
	}

	w, _ = cl.GetDir("/test", 1, 2, 0)
	assert.Equal(t, ents[1], (<-w.C).Path)
	assert.Equal(t, ents[2], (<-w.C).Path)
	assert.Equal(t, (*client.Event)(nil), <-w.C)
	assert.T(t, closed(w.C))
}


func TestDoozerGetDirOffsetLimitBounds(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl := client.New("foo", l.Addr().String())
	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))
	cl.Set("/test/d", store.Clobber, []byte("4"))

	w, _ := cl.GetDir("/test", 1, 5, 0)
	assert.NotEqual(t, (*client.Event)(nil), <-w.C)
	assert.NotEqual(t, (*client.Event)(nil), <-w.C)
	assert.NotEqual(t, (*client.Event)(nil), <-w.C)
	assert.Equal(t, (*client.Event)(nil), <-w.C)
	assert.T(t, closed(w.C))
}


func runDoozer(a ...string) *exec.Cmd {
	path := "/home/kr/src/go/bin/doozerd"
	p, err := exec.Run(
		path,
		append([]string{path}, a...),
		nil,
		"/",
		0,
		0,
		0,
	)
	if err != nil {
		panic(err)
	}
	return p
}

func TestDoozerReconnect(t *testing.T) {
	l := mustListen()
	defer l.Close()
	a := l.Addr().String()
	u := mustListenPacket(a)
	defer u.Close()
	go Main("a", "", u, l, nil)

	l1 := mustListen()
	go Main("a", a, mustListenPacket(l1.Addr().String()), l1, nil)

	l2 := mustListen()
	go Main("a", a, mustListenPacket(l2.Addr().String()), l2, nil)

	c0 := client.New("foo", a)

	_, err := c0.Set("/doozer/slot/2", 0, []byte{})
	assert.Equal(t, nil, err)

	_, err = c0.Set("/doozer/slot/3", 0, []byte{})
	assert.Equal(t, nil, err)

	// Wait for the other members to become CALs.
	for <-c0.Len < 3 {
		time.Sleep(5e8)
	}

	cas, err := c0.Set("/x", -1, []byte{'a'})
	assert.Equal(t, nil, err, err)

	cas, err = c0.Set("/x", -1, []byte{'b'})
	assert.Equal(t, nil, err)

	l1.Close()

	ents, cas, err := c0.Get("/ping", 0)
	assert.Equal(t, nil, err, err)
	assert.NotEqual(t, store.Dir, cas)
	assert.Equal(t, []byte("pong"), ents)

	cas, err = c0.Set("/x", -1, []byte{'c'})
	assert.Equal(t, nil, err, err)

	cas, err = c0.Set("/x", -1, []byte{'d'})
	assert.Equal(t, nil, err)
}
