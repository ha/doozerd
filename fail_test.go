package main

import (
	"github.com/bmizerany/assert"
	"github.com/ha/doozer"
	_ "github.com/ha/doozer"
	"os"
	"os/exec"
	"testing"
	"time"
)

func mustStartDoozer(listen, web, attach, journal string) *exec.Cmd {
	exe, err := exec.LookPath("doozerd")
	if err != nil {
		panic(err)
	}

	args := []string{
		"-l=127.0.0.1:" + listen,
		"-w=127.0.0.1:" + web,
	}

	if attach != "" {
		args = append(args, "-a", "127.0.0.1:"+attach)
	}
	
	if journal != "" {
		args = append(args, "-j", journal)
	}

	cmd := exec.Command(exe, args...)
	err = cmd.Start()
	if err != nil {
		panic(err)
	}

	return cmd
}

func TestDoozerNodeFailure(t *testing.T) {
	d1 := mustStartDoozer("8146", "8180", "", "")
	defer d1.Process.Kill()

	time.Sleep(1e9)

	d2 := mustStartDoozer("8147", "8181", "8146", "")
	defer d2.Process.Kill()
	d3 := mustStartDoozer("8148", "8182", "8146", "")
	defer d3.Process.Kill()

	cl, err := doozer.Dial("127.0.0.1:8146")
	assert.Equal(t, nil, err)

	cl.Set("/ctl/cal/2", 0, nil)
	cl.Set("/ctl/cal/3", 0, nil)

	// Give doozer time to get through initial Nops
	time.Sleep(1e9 * 5)

	// Kill an attached doozer
	d2.Process.Kill()

	// We should get something here
	b, _, err := cl.Get("/ctl/cal/2", nil)
	if err != nil {
		panic(err)
	}
	assert.NotEqual(t, nil, b)
	b, _, err = cl.Get("/ctl/cal/3", nil)
	if err != nil {
		panic(err)
	}
	assert.NotEqual(t, nil, b)
}

func TestDoozerFiveNodeFailure(t *testing.T) {
	d0 := mustStartDoozer("8040", "8880", "", "")
	defer d0.Process.Kill()

	time.Sleep(1e9)

	d1 := mustStartDoozer("8041", "8881", "8040", "")
	defer d1.Process.Kill()
	d2 := mustStartDoozer("8042", "8882", "8040", "")
	defer d2.Process.Kill()
	d3 := mustStartDoozer("8043", "8883", "8040", "")
	defer d3.Process.Kill()
	d4 := mustStartDoozer("8044", "8884", "8040", "")
	defer d4.Process.Kill()

	cl, err := doozer.Dial("127.0.0.1:8040")
	assert.Equal(t, nil, err)

	cl.Set("/ctl/cal/2", 0, nil)
	cl.Set("/ctl/cal/3", 0, nil)

	// Give doozer time to get through initial Nops
	time.Sleep(1e9 * 5)

	// Kill an attached doozer
	d1.Process.Kill()

	// We should get something here
	b, _, err := cl.Get("/ctl/cal/2", nil)
	if err != nil {
		panic(err)
	}
	assert.NotEqual(t, nil, b)
	b, _, err = cl.Get("/ctl/cal/3", nil)
	if err != nil {
		panic(err)
	}
	assert.NotEqual(t, nil, b)
}

func TestDoozerNodeFailureJournaled(t *testing.T) {
	d1 := mustStartDoozer("8246", "8280", "", "1.journal")
	defer d1.Process.Kill()
	defer os.Remove("1.journal")

	time.Sleep(1e9)

	d2 := mustStartDoozer("8247", "8281", "8246", "2.journal")
	defer d2.Process.Kill()
	defer os.Remove("2.journal")
	d3 := mustStartDoozer("8248", "8282", "8246", "3.journal")
	defer d3.Process.Kill()
	defer os.Remove("3.journal")

	cl, err := doozer.Dial("127.0.0.1:8246")
	assert.Equal(t, nil, err)

	cl.Set("/ctl/cal/2", 0, nil)
	cl.Set("/ctl/cal/3", 0, nil)

	// Give doozer time to get through initial Nops
	time.Sleep(1e9 * 5)

	// Kill an attached doozer
	d2.Process.Kill()

	// We should get something here
	b, _, err := cl.Get("/ctl/cal/2", nil)
	if err != nil {
		panic(err)
	}
	assert.NotEqual(t, nil, b)
	b, _, err = cl.Get("/ctl/cal/3", nil)
	if err != nil {
		panic(err)
	}
	assert.NotEqual(t, nil, b)
}
