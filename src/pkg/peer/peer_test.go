package peer

import (
	"doozer/store"
	_ "doozer/quiet"
	"github.com/ha/doozer"
	"exec"
	"github.com/bmizerany/assert"
	"net"
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


func TestDoozerNop(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())
	err := cl.Nop()
	assert.Equal(t, nil, err)
}


func TestDoozerGet(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

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

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

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

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

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


func TestDoozerWaitSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())
	var rev int64 = 1

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	ev, err := cl.Wait("/test/**", rev)
	assert.Equal(t, nil, err)
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, []byte("bar"), ev.Body)
	assert.T(t, ev.IsSet())
	rev = ev.Rev + 1

	cl.Set("/test/fun", store.Clobber, []byte("house"))
	ev, err = cl.Wait("/test/**", rev)
	assert.Equal(t, nil, err)
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, []byte("house"), ev.Body)
	assert.T(t, ev.IsSet())
	rev = ev.Rev + 1

	cl.Del("/test/foo", store.Clobber)
	ev, err = cl.Wait("/test/**", rev)
	assert.Equal(t, nil, err)
	assert.Equal(t, "/test/foo", ev.Path)
	assert.T(t, ev.IsDel())
}


func TestDoozerWaitWithRev(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

	// Create some history
	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	cl.Set("/test/fun", store.Clobber, []byte("house"))

	ev, err := cl.Wait("/test/**", 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, "/test/foo", ev.Path)
	assert.Equal(t, []byte("bar"), ev.Body)
	assert.T(t, ev.IsSet())
	rev := ev.Rev + 1

	ev, err = cl.Wait("/test/**", rev)
	assert.Equal(t, nil, err)
	assert.Equal(t, "/test/fun", ev.Path)
	assert.Equal(t, []byte("house"), ev.Body)
	assert.T(t, ev.IsSet())
}


func TestDoozerWalk(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	cl.Set("/test/fun", store.Clobber, []byte("house"))

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	info, err := cl.Walk("/test/**", rev, 0, -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(info))

	assert.Equal(t, "/test/foo", info[0].Path)
	assert.Equal(t, "bar", string(info[0].Body))
	assert.T(t, info[0].IsSet())

	assert.Equal(t, "/test/fun", info[1].Path)
	assert.Equal(t, "house", string(info[1].Body))
	assert.T(t, info[1].IsSet())
}


func TestDoozerWalkWithRev(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

	rev, _ := cl.Set("/test/foo", store.Clobber, []byte("bar"))
	cl.Set("/test/fun", store.Clobber, []byte("house"))
	cl.Set("/test/fab", store.Clobber, []byte("ulous"))

	info, err := cl.Walk("/test/**", rev, 0, -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(info))
	assert.Equal(t, "/test/foo", info[0].Path)
}

func TestDoozerWalkWithOffsetAndLimit(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("abc"))
	cl.Set("/test/b", store.Clobber, []byte("def"))
	cl.Set("/test/c", store.Clobber, []byte("ghi"))
	cl.Set("/test/d", store.Clobber, []byte("jkl"))

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	info, err := cl.Walk("/test/**", rev, 1, 2)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(info))

	assert.Equal(t, "/test/b", info[0].Path)
	assert.Equal(t, "def", string(info[0].Body))
	assert.T(t, info[0].IsSet())

	assert.Equal(t, "/test/c", info[1].Path)
	assert.Equal(t, "ghi", string(info[1].Body))
	assert.T(t, info[1].IsSet())
}

func TestDoozerStat(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

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

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	got, err := cl.Getdir("/test", rev, 0, -1)
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestDoozerGetdirOnFile(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("1"))

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	names, err := cl.Getdir("/test/a", rev, 0, -1)
	assert.Equal(t, &doozer.Error{doozer.ErrNotDir, ""}, err)
	assert.Equal(t, []string{}, names)
}

func TestDoozerGetdirMissing(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	names, err := cl.Getdir("/not/here", rev, 0, -1)
	assert.Equal(t, &doozer.Error{doozer.ErrNoEnt, ""}, err)
	assert.Equal(t, []string{}, names)
}

func TestDoozerGetdirOffsetLimit(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", nil, u, l, nil, 1e9, 2e9, 3e9)

	cl := dial(l.Addr().String())
	cl.Set("/test/a", store.Clobber, []byte("1"))
	cl.Set("/test/b", store.Clobber, []byte("2"))
	cl.Set("/test/c", store.Clobber, []byte("3"))
	cl.Set("/test/d", store.Clobber, []byte("4"))

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	names, err := cl.Getdir("/test", rev, 1, 2)
	assert.Equal(t, nil, err)
	assert.Equal(t, []string{"b", "c"}, names)
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
