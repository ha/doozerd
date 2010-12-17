package test

import (
	"doozer/client"
	"exec"
	"github.com/bmizerany/assert"
	"syscall"
	"testing"
	"time"
)

func mustRunDoozer(listen, web, attach string) *exec.Cmd {
	exe, err := exec.LookPath("doozerd")
	if err != nil {
		panic(err)
	}

	args := []string{
		"doozerd",
		"-l=127.0.0.1:" + listen,
		"-w=127.0.0.1:" + web,
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
	<-ch
	<-ch
	cl.Set("/doozer/slot/3", "", "")
	<-ch
	<-ch

	// Give doozer time to get through initial Nops
	time.Sleep(1e9 * 60)

	// Kill an attached doozer
	syscall.Kill(d2.Pid, 9)

	// We should get something here
	ev := <-ch
	assert.NotEqual(t, nil, ev)

	for i := 0; i < 1000; i++ {
		cl.Noop()
	}
}

func TestDoozerFiveNodeFailure(t *testing.T) {
	d0 := mustRunDoozer("8040", "8080", "")
	defer syscall.Kill(d0.Pid, 9)

	time.Sleep(1e9)

	d1 := mustRunDoozer("8041", "8081", "8040")
	defer syscall.Kill(d1.Pid, 9)
	d2 := mustRunDoozer("8042", "8082", "8040")
	defer syscall.Kill(d2.Pid, 9)
	d3 := mustRunDoozer("8043", "8083", "8040")
	defer syscall.Kill(d3.Pid, 9)
	d4 := mustRunDoozer("8044", "8084", "8040")
	defer syscall.Kill(d4.Pid, 9)

	cl, err := client.Dial("127.0.0.1:8040")
	assert.Equal(t, nil, err)

	ch, err := cl.Watch("/doozer/slot/*")
	assert.Equal(t, nil, err)

	cl.Set("/doozer/slot/2", "", "")
	<-ch
	<-ch
	cl.Set("/doozer/slot/3", "", "")
	<-ch
	<-ch

	// Give doozer time to get through initial Nops
	time.Sleep(1e9 * 60)

	// Kill an attached doozer
	syscall.Kill(d1.Pid, 9)

	// We should get something here
	ev := <-ch
	assert.NotEqual(t, nil, ev)

	for i := 0; i < 1000; i++ {
		cl.Noop()
	}
}
