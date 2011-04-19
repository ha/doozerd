package doozer

import (
	"doozer/store"
	_ "doozer/quiet"
	"exec"
	"github.com/bmizerany/assert"
	"github.com/ha/doozer"
	"net"
	"os"
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


func TestDoozerNop(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())
	err := cl.Nop()
	assert.Equal(t, nil, err)
}


func TestDoozerGet(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	_, err := cl.Set("/x", store.Missing, []byte{'a'})
	assert.Equal(t, nil, err)

	ents, rev, err := cl.Get("/x", nil)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, store.Dir, rev)
	assert.Equal(t, []byte{'a'}, ents)

	//cl.Set("/test/a", store.Missing, []byte{'1'})
	//cl.Set("/test/b", store.Missing, []byte{'2'})
	//cl.Set("/test/c", store.Missing, []byte{'3'})

	//ents, rev, err = cl.Get("/test", 0)
	//sort.SortStrings(ents)
	//assert.Equal(t, store.Dir, rev)
	//assert.Equal(t, nil, err)
	//assert.Equal(t, []string{"a", "b", "c"}, ents)
}


func TestDoozerSet(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	for i := byte(0); i < 10; i++ {
		_, err := cl.Set("/x", store.Clobber, []byte{'0' + i})
		assert.Equal(t, nil, err)
	}
}


func TestDoozerGetWithRev(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	rev1, err := cl.Set("/x", store.Missing, []byte{'a'})
	assert.Equal(t, nil, err)

	v, rev, err := cl.Get("/x", &rev1) // Use the snapshot.
	assert.Equal(t, nil, err)
	assert.Equal(t, rev1, rev)
	assert.Equal(t, []byte{'a'}, v)

	rev2, err := cl.Set("/x", rev, []byte{'b'})
	assert.Equal(t, nil, err)

	v, rev, err = cl.Get("/x", nil) // Read the new value.
	assert.Equal(t, nil, err)
	assert.Equal(t, rev2, rev)
	assert.Equal(t, []byte{'b'}, v)

	v, rev, err = cl.Get("/x", &rev1) // Read the saved value again.
	assert.Equal(t, nil, err)
	assert.Equal(t, rev1, rev)
	assert.Equal(t, []byte{'a'}, v)
}


func TestDoozerWatchSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	w, err := cl.Watch("/test/**", 0)
	assert.Equal(t, nil, err, err)
	defer w.Cancel()

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	ev := <-w.C
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, []byte("bar"), ev.Body)
	assert.T(t, ev.IsSet())

	cl.Set("/test/fun", store.Clobber, []byte("house"))
	ev = <-w.C
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, []byte("house"), ev.Body)
	assert.T(t, ev.IsSet())

	cl.Del("/test/foo", store.Clobber)
	ev = <-w.C
	assert.Equal(t, "/test/foo", ev.Path)
	assert.T(t, ev.IsDel())

	w.Cancel()
	ev = <-w.C
	assert.Tf(t, closed(w.C), "got %v", ev)
}


func TestDoozerWatchWithRev(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	// Create some history
	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	cl.Set("/test/fun", store.Clobber, []byte("house"))

	// Ask doozer for the history
	w, err := cl.Watch("/test/**", 1)
	assert.Equal(t, nil, err, err)
	defer w.Cancel()

	ev := <-w.C
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, []byte("bar"), ev.Body)
	assert.T(t, ev.IsSet())

	ev = <-w.C
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, []byte("house"), ev.Body)
	assert.T(t, ev.IsSet())
}


func TestDoozerWalk(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	cl.Set("/test/fun", store.Clobber, []byte("house"))

	w, err := cl.Walk("/test/**", nil, nil, nil)
	assert.Equal(t, nil, err, err)

	ev := <-w.C
	assert.NotEqual(t, (*doozer.Event)(nil), ev)
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, "bar", string(ev.Body))
	assert.T(t, ev.IsSet())

	ev = <-w.C
	assert.NotEqual(t, (*doozer.Event)(nil), ev)
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, "house", string(ev.Body))
	assert.T(t, ev.IsSet())

	ev = <-w.C
	assert.Tf(t, closed(w.C), "got %v", ev)
}


func TestDoozerWalkWithRev(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	rev, _ := cl.Set("/test/foo", store.Clobber, []byte("bar"))
	cl.Set("/test/fun", store.Clobber, []byte("house"))
	cl.Set("/test/fab", store.Clobber, []byte("ulous"))

	w, err := cl.Walk("/test/**", &rev, nil, nil)
	assert.Equal(t, nil, err, err)

	ls := []string{}
	for e := range w.C {
		ls = append(ls, e.Path)
	}

	sort.SortStrings(ls)
	assert.Equal(t, []string{"/test/foo"}, ls)
}

func TestDoozerWalkWithOffsetAndLimit(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("abc"))
	cl.Set("/test/b", store.Clobber, []byte("def"))
	cl.Set("/test/c", store.Clobber, []byte("ghi"))
	cl.Set("/test/d", store.Clobber, []byte("jkl"))

	offset := int32(1)
	limit := int32(2)

	w, err := cl.Walk("/test/**", nil, &offset, &limit)
	assert.Equal(t, nil, err, err)

	ev := <-w.C
	assert.NotEqual(t, (*doozer.Event)(nil), ev)
	assert.Equal(t, "/test/b", ev.Path)
	assert.Equal(t, "def", string(ev.Body))
	assert.T(t, ev.IsSet())

	ev = <-w.C
	assert.NotEqual(t, (*doozer.Event)(nil), ev)
	assert.Equal(t, "/test/c", ev.Path)
	assert.Equal(t, "ghi", string(ev.Body))
	assert.T(t, ev.IsSet())

	ev = <-w.C
	assert.Tf(t, closed(w.C), "got %v", ev)
}

func TestDoozerStat(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	setRev, _ := cl.Set("/test/fun", store.Clobber, []byte("house"))

	ln, rev, err := cl.Stat("/test", nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, store.Dir, rev)
	assert.Equal(t, int32(2), ln)

	ln, rev, err = cl.Stat("/test/fun", nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, setRev, rev)
	assert.Equal(t, int32(5), ln)
}

func TestDoozerGetdirOnDir(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))

	w, err := cl.Getdir("/test", 0, 0, nil)
	assert.Equal(t, nil, err)

	got := make([]string, 0)
	for e := range w.C {
		got = append(got, e.Path)
	}

	sort.SortStrings(got)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestDoozerGetdirOnFile(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("1"))

	w, err := cl.Getdir("/test/a", 0, 0, nil)
	assert.Equal(t, nil, err)

	assert.Equal(t, os.ENOTDIR, (<-w.C).Err)
}

func TestDoozerGetdirMissing(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())

	w, err := cl.Getdir("/not/here", 0, 0, nil)
	assert.Equal(t, nil, err)

	assert.Equal(t, os.ENOENT, (<-w.C).Err)
}

func TestDoozerGetdirOffsetLimit(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())
	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))
	cl.Set("/test/d", store.Clobber, []byte("4"))

	// The order is arbitrary.  We need to collect them
	// because it's not safe to assume the order.
	w, _ := cl.Getdir("/test", 0, 0, nil)
	ents := make([]string, 0)
	for e := range w.C {
		ents = append(ents, e.Path)
	}

	w, _ = cl.Getdir("/test", 1, 2, nil)
	assert.Equal(t, ents[1], (<-w.C).Path)
	assert.Equal(t, ents[2], (<-w.C).Path)
	assert.Equal(t, (*doozer.Event)(nil), <-w.C)
	assert.T(t, closed(w.C))
}


func TestDoozerGetdirOffsetLimitBounds(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	cl := doozer.New("foo", l.Addr().String())
	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))
	cl.Set("/test/d", store.Clobber, []byte("4"))

	w, _ := cl.Getdir("/test", 1, 5, nil)
	assert.NotEqual(t, (*doozer.Event)(nil), <-w.C)
	assert.NotEqual(t, (*doozer.Event)(nil), <-w.C)
	assert.NotEqual(t, (*doozer.Event)(nil), <-w.C)
	assert.Equal(t, (*doozer.Event)(nil), <-w.C)
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
	go Main("a", "", u, l, nil, 1e9, 2e9, 3e9)

	l1 := mustListen()
	go Main("a", a, mustListenPacket(l1.Addr().String()), l1, nil, 1e9, 2e9, 3e9)

	l2 := mustListen()
	go Main("a", a, mustListenPacket(l2.Addr().String()), l2, nil, 1e9, 2e9, 3e9)

	c0 := doozer.New("foo", a)

	_, err := c0.Set("/ctl/cal/2", 0, []byte{})
	assert.Equal(t, nil, err)

	_, err = c0.Set("/ctl/cal/3", 0, []byte{})
	assert.Equal(t, nil, err)

	// Wait for the other nodes to become CALs.
	for <-c0.Len < 3 {
		time.Sleep(5e8)
	}

	rev, err := c0.Set("/x", -1, []byte{'a'})
	assert.Equal(t, nil, err, err)

	rev, err = c0.Set("/x", -1, []byte{'b'})
	assert.Equal(t, nil, err)

	l1.Close()

	ents, rev, err := c0.Get("/x", nil)
	assert.Equal(t, nil, err, err)
	assert.NotEqual(t, store.Dir, rev)
	assert.Equal(t, []byte{'b'}, ents)

	rev, err = c0.Set("/x", -1, []byte{'c'})
	assert.Equal(t, nil, err, err)

	rev, err = c0.Set("/x", -1, []byte{'d'})
	assert.Equal(t, nil, err)
}


func TestDoozerRandIdHasNoPadding(t *testing.T) {
	s := randId()
	assert.T(t, len(s) > 0)
	assert.NotEqual(t, s[len(s)-1], '=')
}
