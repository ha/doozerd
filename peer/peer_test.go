package peer

import (
	"github.com/ha/doozerd/store"
	"github.com/bmizerany/assert"
	"github.com/ha/doozer"
	"os/exec"

	"testing"
)

func TestDoozerNop(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

	cl := dial(l.Addr().String())
	err := cl.Nop()
	assert.Equal(t, nil, err)
}

func TestDoozerGet(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

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
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

	cl := dial(l.Addr().String())

	for i := byte(0); i < 10; i++ {
		_, err := cl.Set("/x", store.Clobber, []byte{'0' + i})
		assert.Equal(t, nil, err)
	}

	_, err := cl.Set("/x", 0, []byte{'X'})
	assert.Equal(t, &doozer.Error{doozer.ErrOldRev, ""}, err)
}

func TestDoozerGetWithRev(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

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
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

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
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

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

func TestDoozerStat(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

	cl := dial(l.Addr().String())

	cl.Set("/test/foo", store.Clobber, []byte("bar"))
	setRev, _ := cl.Set("/test/fun", store.Clobber, []byte("house"))

	ln, rev, err := cl.Stat("/test", nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, store.Dir, rev)
	assert.Equal(t, int(2), ln)

	ln, rev, err = cl.Stat("/test/fun", nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, setRev, rev)
	assert.Equal(t, int(5), ln)
}

func TestDoozerGetdirOnDir(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

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
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

	cl := dial(l.Addr().String())

	cl.Set("/test/a", store.Clobber, []byte("1"))

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	names, err := cl.Getdir("/test/a", rev, 0, -1)
	assert.Equal(t, &doozer.Error{doozer.ErrNotDir, ""}, err)
	assert.Equal(t, []string(nil), names)
}

func TestDoozerGetdirMissing(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

	cl := dial(l.Addr().String())

	rev, err := cl.Rev()
	if err != nil {
		panic(err)
	}

	names, err := cl.Getdir("/not/here", rev, 0, -1)
	assert.Equal(t, &doozer.Error{doozer.ErrNoEnt, ""}, err)
	assert.Equal(t, []string(nil), names)
}

func TestDoozerGetdirOffsetLimit(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenUDP(l.Addr().String())
	defer u.Close()

	go Main("a", "X", "", "", "", "", nil, u, l, nil, 1e9, 2e9, 3e9, 101)

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

func TestPeerShun(t *testing.T) {
	l0 := mustListen()
	defer l0.Close()
	a0 := l0.Addr().String()
	u0 := mustListenUDP(a0)
	defer u0.Close()

	l1 := mustListen()
	defer l1.Close()
	u1 := mustListenUDP(l1.Addr().String())
	defer u1.Close()
	l2 := mustListen()
	defer l2.Close()
	u2 := mustListenUDP(l2.Addr().String())
	defer u2.Close()

	go Main("a", "X", "", "", "", "", nil, u0, l0, nil, 1e8, 1e7, 1e9, 1e9)
	go Main("a", "Y", "", "", "", "", dial(a0), u1, l1, nil, 1e8, 1e7, 1e9, 1e9)
	go Main("a", "Z", "", "", "", "", dial(a0), u2, l2, nil, 1e8, 1e7, 1e9, 1e9)

	cl := dial(l0.Addr().String())
	cl.Set("/ctl/cal/1", store.Missing, nil)
	cl.Set("/ctl/cal/2", store.Missing, nil)

	waitFor(cl, "/ctl/node/X/writable")
	waitFor(cl, "/ctl/node/Y/writable")
	waitFor(cl, "/ctl/node/Z/writable")

	rev, err := cl.Set("/test", store.Clobber, nil)
	if e, ok := err.(*doozer.Error); ok && e.Err == doozer.ErrReadonly {
	} else if err != nil {
		panic(err)
	}

	u1.Close()
	for {
		ev, err := cl.Wait("/ctl/cal/*", rev)
		if err != nil {
			panic(err)
		}
		if ev.IsSet() && len(ev.Body) == 0 {
			break
		}
		rev = ev.Rev + 1
	}
}

func assertDenied(t *testing.T, err error) {
	assert.NotEqual(t, nil, err)
	assert.Equal(t, doozer.ErrOther, err.(*doozer.Error).Err)
	assert.Equal(t, "permission denied", err.(*doozer.Error).Detail)
}

func runDoozer(a ...string) *exec.Cmd {
	path := "/home/kr/src/go/bin/doozerd"
	args := append([]string{path}, a...)
	c := exec.Command(path, args...)
	if err := c.Run(); err != nil {
		panic(err)
	}
	return c
}
