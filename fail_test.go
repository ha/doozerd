package main

import (
	"bytes"
	"github.com/bmizerany/assert"
	"github.com/ha/doozer"
	_ "github.com/ha/doozer"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
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
	defer os.Remove("1.journal")

	time.Sleep(1e9)

	d2 := mustStartDoozer("8247", "8281", "8246", "2.journal")
	defer os.Remove("2.journal")
	d3 := mustStartDoozer("8248", "8282", "8246", "3.journal")
	defer os.Remove("3.journal")

	cl, err := doozer.Dial("127.0.0.1:8246")
	assert.Equal(t, nil, err)
	
	cl.Set("/ctl/cal/2", 0, nil)
	cl.Set("/ctl/cal/3", 0, nil)

	// Let's save some data to retrieve later.
	r := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	testData := make([][]byte, 64)
	for i := 0; i < 3; i++ {
		b := make([]byte, 100)
		testData[i] = b
		for j := 0; j < len(b); j++ {
			b[j] = '0' + byte(r.Intn(50))
		}
		_, err := cl.Set("/t/t" + strconv.Itoa(i), 100+int64(i), b)
		if err != nil {
			t.Fatal(err, "/t/t" + strconv.Itoa(i))
		}
	}

	// Give doozer time to get through initial Nops
	time.Sleep(1e9 * 5)

	// Kill an attached doozer
	d2.Process.Kill()

	// Do we still have the data?
	for i := 0; i < 3; i++ {
		b, _, err := cl.Get("/t/t"+ strconv.Itoa(i), nil)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(b, testData[i]) == false {
			t.Fatal("missing data in store")
		}
	}

	// We should get something here
	b, _, err := cl.Get("/ctl/cal/2", nil)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEqual(t, nil, b)
	b, _, err = cl.Get("/ctl/cal/3", nil)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEqual(t, nil, b)

	cl.Close()

	// Kill the other doozers.
	d3.Process.Kill()
	time.Sleep(1e9)
	d1.Process.Kill()
	
	// Restart cluster from journal.
	d1 = mustStartDoozer("8246", "8280", "", "1.journal")
	defer d1.Process.Kill()
	time.Sleep(1e9)
	d2 = mustStartDoozer("8247", "8281", "8246", "2.journal")
	defer d2.Process.Kill()
	d3 = mustStartDoozer("8248", "8282", "8246", "3.journal")
	defer d3.Process.Kill()
	
	cl, err = doozer.Dial("127.0.0.1:8246")
	assert.Equal(t, nil, err)	
	
	// Do we still have the data?
	for i := 0; i < 3; i++ {
		b, _, err := cl.Get("/t/t"+ strconv.Itoa(i), nil)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(b, testData[i]) == false {
			t.Fatal("missing data in store: ", i, b, testData[i])
		}
	}	
}
