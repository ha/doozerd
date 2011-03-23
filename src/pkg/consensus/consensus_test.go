package consensus

import (
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"os"
	"testing"
)


func TestConsensusOne(t *testing.T) {
	self := "test"
	alpha := int64(1)
	st := store.New()

	st.Ops <- store.Op{1, store.MustEncodeSet("/ctl/node/"+self+"/addr", "x", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/1", self, 0)}
	<-st.Seqns

	cmw := st.Watch(store.Any)
	in := make(chan Packet)
	out := make(chan Packet)
	seqns := make(chan int64, int(alpha))
	props := make(chan *Prop)

	NewManager(self, 0, alpha, in, out, st.Ops, seqns, props, cmw, 10e9, st)

	go func() {
		for o := range out {
			in <- o
		}
	}()

	for i := int64(3); i <= alpha+2; i++ {
		st.Ops <- store.Op{Seqn: i, Mut: store.Nop}
	}

	n := <-seqns
	w, err := st.Wait(n)
	if err != nil {
		panic(err)
	}
	props <- &Prop{n, []byte("foo")}
	e := <-w

	exp := store.Event{
		Seqn: 4,
		Path: "/ctl/err",
		Body: "bad mutation",
		Rev:  4,
		Mut:  "foo",
		Err:  os.NewError("bad mutation"),
	}

	e.Getter = nil
	assert.Equal(t, exp, e)
}


func TestConsensusTwo(t *testing.T) {
	a := "a"
	b := "b"
	alpha := int64(1)
	st := store.New()

	st.Ops <- store.Op{1, store.MustEncodeSet("/ctl/node/"+a+"/addr", "x", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/1", a, 0)}
	st.Ops <- store.Op{3, store.MustEncodeSet("/ctl/node/"+b+"/addr", "x", 0)}
	st.Ops <- store.Op{4, store.MustEncodeSet("/ctl/cal/2", b, 0)}
	snn := <-st.Seqns

	acmw := st.Watch(store.Any)
	ain := make(chan Packet)
	aout := make(chan Packet)
	aseqns := make(chan int64, int(alpha))
	aprops := make(chan *Prop)
	NewManager(a, 0, alpha, ain, aout, st.Ops, aseqns, aprops, acmw, 10e9, st)

	bcmw := st.Watch(store.Any)
	bin := make(chan Packet)
	bout := make(chan Packet)
	bseqns := make(chan int64, int(alpha))
	bprops := make(chan *Prop)
	NewManager(b, 0, alpha, bin, bout, st.Ops, bseqns, bprops, bcmw, 10e9, st)

	go func() {
		for o := range aout {
			o.Addr = a
			ain <- o
			bin <- o
		}
	}()

	go func() {
		for o := range bout {
			o.Addr = b
			ain <- o
			bin <- o
		}
	}()

	for i := snn + 1; i < snn+1+alpha; i++ {
		st.Ops <- store.Op{Seqn: i, Mut: store.Nop}
	}

	n := <-aseqns
	w, err := st.Wait(n)
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


func TestLearnedValueIsLearned(t *testing.T) {
	self := "test"
	alpha := int64(1)
	st := store.New()

	st.Ops <- store.Op{1, store.MustEncodeSet("/ctl/node/"+self+"/addr", "x", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/ctl/cal/1", self, 0)}
	<-st.Seqns

	cmw := st.Watch(store.Any)
	in := make(chan Packet)
	out := make(chan Packet)
	seqns := make(chan int64, int(alpha))
	props := make(chan *Prop)

	NewManager(self, 0, alpha, in, out, st.Ops, seqns, props, cmw, 10e9, st)

	v := store.MustEncodeSet("/foo", "bar", -1)
	st.Ops <- store.Op{Seqn: 3, Mut: v}

	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: rsvp})}
	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: nominate})}
	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: vote})}
	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: nop})}
	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: tick})}
	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: learn})}
	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: propose})}
	in <- Packet{"x", mustMarshal(&M{Seqn: proto.Int64(3), Cmd: invite})}
	exp := Packet{"x", mustMarshal(&M{
		Cmd:   learn,
		Seqn:  proto.Int64(3),
		Value: []byte(v),
	})}

	assert.Equal(t, exp, <-out)
}
