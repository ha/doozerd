package consensus

import (
	"errors"
	"github.com/bmizerany/assert"
	"github.com/ha/doozerd/store"
	"net"
	"testing"
	"time"
)

func TestConsensusOne(t *testing.T) {
	self := "test"
	const alpha = 1
	st := store.New()

	st.Ops <- store.Op{1, store.MustEncodeSet("/ctl/node/"+self+"/addr", "1.2.3.4:5", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/1", self, 0)}
	<-st.Seqns

	in := make(chan Packet)
	out := make(chan Packet)
	seqns := make(chan int64, alpha)
	props := make(chan *Prop)

	m := &Manager{
		Self:   self,
		DefRev: 2,
		Alpha:  alpha,
		In:     in,
		Out:    out,
		Ops:    st.Ops,
		PSeqn:  seqns,
		Props:  props,
		TFill:  10e9,
		Store:  st,
		Ticker: time.Tick(10e6),
	}
	go m.Run()

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
		Err:  errors.New("bad mutation"),
	}

	e.Getter = nil
	assert.Equal(t, exp, e)
}

func TestConsensusTwo(t *testing.T) {
	a := "a"
	b := "b"
	x, _ := net.ResolveUDPAddr("udp", "1.2.3.4:5")
	xs := "1.2.3.4:5"
	y, _ := net.ResolveUDPAddr("udp", "2.3.4.5:6")
	ys := "2.3.4.5:6"
	const alpha = 1
	st := store.New()

	st.Ops <- store.Op{1, store.Nop}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/node/a/addr", xs, 0)}
	st.Ops <- store.Op{3, store.MustEncodeSet("/ctl/cal/1", a, 0)}
	st.Ops <- store.Op{4, store.MustEncodeSet("/ctl/node/b/addr", ys, 0)}
	st.Ops <- store.Op{5, store.MustEncodeSet("/ctl/cal/2", b, 0)}

	ain := make(chan Packet)
	aout := make(chan Packet)
	aseqns := make(chan int64, alpha)
	aprops := make(chan *Prop)
	am := &Manager{
		Self:   a,
		DefRev: 5,
		Alpha:  alpha,
		In:     ain,
		Out:    aout,
		Ops:    st.Ops,
		PSeqn:  aseqns,
		Props:  aprops,
		TFill:  10e9,
		Store:  st,
		Ticker: time.Tick(10e6),
	}
	go am.Run()

	bin := make(chan Packet)
	bout := make(chan Packet)
	bseqns := make(chan int64, alpha)
	bprops := make(chan *Prop)
	bm := &Manager{
		Self:   b,
		DefRev: 5,
		Alpha:  alpha,
		In:     bin,
		Out:    bout,
		Ops:    st.Ops,
		PSeqn:  bseqns,
		Props:  bprops,
		TFill:  10e9,
		Store:  st,
		Ticker: time.Tick(10e6),
	}
	go bm.Run()

	go func() {
		for o := range aout {
			if o.Addr.Port == x.Port && o.Addr.IP.Equal(x.IP) {
				go func(o Packet) { ain <- o }(o)
			} else {
				o.Addr = x
				go func(o Packet) { bin <- o }(o)
			}
		}
	}()

	go func() {
		for o := range bout {
			if o.Addr.Port == y.Port && o.Addr.IP.Equal(y.IP) {
				go func(o Packet) { bin <- o }(o)
			} else {
				o.Addr = y
				go func(o Packet) { ain <- o }(o)
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
		Err:  errors.New("bad mutation"),
	}

	e.Getter = nil
	assert.Equal(t, exp, e)
}
