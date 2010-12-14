package doozer

import (
	"doozer/client"
	"exec"
	"github.com/bmizerany/assert"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"
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

func TestDoozerSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()
	u := mustListenPacket(l.Addr().String())
	defer u.Close()

	go Main("a", "", u, l, nil)

	cl, err := client.Dial(l.Addr().String())
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, cl.Noop())
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

func mustRunDoozer(listen, web, attach string) *exec.Cmd {
	exe, err := exec.LookPath("doozerd")
	if err != nil {
		panic(err)
	}

	args := []string{
		"doozerd",
		"-l=127.0.0.1:"+listen,
		"-w=127.0.0.1:"+web,
	}

	if attach != "" {
		args = append(args, "-a", "127.0.0.1:"+attach)
	}

	cmd, err := exec.Run(
		exe,
		args,
		nil,
		".",
		exec.PassThrough,
		exec.PassThrough,
		exec.PassThrough,
	)
	if err != nil {
		panic(err)
	}

	return cmd
}

func TestDoozerNodeFailure(t *testing.T) {
	d1 := mustRunDoozer("8046", "8080", "")
	defer syscall.Kill(d1.Pid, 9)

	time.Sleep(1e9)

	d2 := mustRunDoozer("8047", "8081", "8046")
	defer syscall.Kill(d2.Pid, 9)
	d3 := mustRunDoozer("8048", "8082", "8046")
	defer syscall.Kill(d3.Pid, 9)

	cl, err := client.Dial("127.0.0.1:8046")
	assert.Equal(t, nil, err)

	ch, err := cl.Watch("/doozer/slot/*")
	assert.Equal(t, nil, err)

	cl.Set("/doozer/slot/2", "", "")
	<-ch; <-ch
	cl.Set("/doozer/slot/3", "", "")
	<-ch; <-ch

	// Give doozer time to get through initial Nops
	time.Sleep(1e9*60)

	// Kill an attached doozer
	syscall.Kill(d2.Pid, 9)


	// We should get something here
	ev := <-ch
	assert.NotEqual(t, nil, ev)

	for i := 0; i < 1000; i++ {
		cl.Noop()
	}
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
