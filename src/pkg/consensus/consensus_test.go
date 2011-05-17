package consensus

import (
	"doozer/store"
	"github.com/bmizerany/assert"
	"os"
	"testing"
	"time"
)


func TestConsensusOne(t *testing.T) {
	self := "test"
	const alpha = 1
	st := store.New()

	st.Ops <- store.Op{1, store.MustEncodeSet("/ctl/node/"+self+"/addr", "x", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/1", self, 0)}
	<-st.Seqns

	in := make(chan Packet)
	out := make(chan Packet)
	seqns := make(chan int64, alpha)
	props := make(chan *Prop)

	cfg := &Config{
		self,
		2,
		alpha,
		in,
		out,
		st.Ops,
		seqns,
		props,
		10e9,
		st,
		time.Tick(10e6),
	}
	NewManager(cfg)

	go func() {
		for o := range out {
			in <- o
		}
	}()

	n := <-seqns
	w, err := st.Wait(store.Any, n)
	if err != nil {
		panic(err)
	}
	props <- &Prop{n, []byte("foo")}
	e := <-w

	exp := store.Event{
		Seqn: 3,
		Path: "/ctl/err",
		Body: "bad mutation",
		Rev:  3,
		Mut:  "foo",
		Err:  os.NewError("bad mutation"),
	}

	e.Getter = nil
	assert.Equal(t, exp, e)
}


func TestConsensusTwo(t *testing.T) {
	a := "a"
	b := "b"
	const alpha = 1
	st := store.New()

	st.Ops <- store.Op{1, store.Nop}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/node/"+a+"/addr", "x", 0)}
	st.Ops <- store.Op{3, store.MustEncodeSet("/ctl/cal/1", a, 0)}
	st.Ops <- store.Op{4, store.MustEncodeSet("/ctl/node/"+b+"/addr", "y", 0)}
	st.Ops <- store.Op{5, store.MustEncodeSet("/ctl/cal/2", b, 0)}

	ain := make(chan Packet)
	aout := make(chan Packet)
	aseqns := make(chan int64, alpha)
	aprops := make(chan *Prop)
	acfg := &Config{
		a,
		5,
		alpha,
		ain,
		aout,
		st.Ops,
		aseqns,
		aprops,
		10e9,
		st,
		time.Tick(10e6),
	}
	NewManager(acfg)

	bin := make(chan Packet)
	bout := make(chan Packet)
	bseqns := make(chan int64, alpha)
	bprops := make(chan *Prop)
	bcfg := &Config{
		b,
		5,
		alpha,
		bin,
		bout,
		st.Ops,
		bseqns,
		bprops,
		10e9,
		st,
		time.Tick(10e6),
	}
	NewManager(bcfg)

	go func() {
		for o := range aout {
			o := o
			if o.Addr == "x" {
				go func() { ain <- o }()
			} else {
				o.Addr = "x"
				go func() { bin <- o }()
			}
		}
	}()

	go func() {
		for o := range bout {
			if o.Addr == "y" {
				go func() { bin <- o }()
			} else {
				o.Addr = "y"
				go func() { ain <- o }()
			}
		}
	}()

	n := <-aseqns
	assert.Equal(t, int64(6), n)
	w, err := st.Wait(store.Any, n)
	if err != nil {
		panic(err)
	}
	aprops <- &Prop{n, []byte("foo")}
	e := <-w

	exp := store.Event{
		Seqn: 6,
		Path: "/ctl/err",
		Body: "bad mutation",
		Rev:  6,
		Mut:  "foo",
		Err:  os.NewError("bad mutation"),
	}

	e.Getter = nil
	assert.Equal(t, exp, e)
}
