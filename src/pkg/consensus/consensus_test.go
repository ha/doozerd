package consensus

import (
	"doozer/store"
	"github.com/bmizerany/assert"
	"os"
	"testing"
)

func TestConsensusSimple(t *testing.T) {
	self := "test"
	alpha := int64(1)
	st := store.New()

	st.Ops <- store.Op{1, store.MustEncodeSet("/doozer/info/"+self+"/addr", "x", 0)}
	st.Ops <- store.Op{2, store.MustEncodeSet("/doozer/slot/1", self, 0)}
	<-st.Wait(2)

	cmw := st.Watch(store.Any)
	in := make(chan Packet)
	out := make(chan Packet)
	seqns := make(chan int64, int(alpha))
	props := make(chan *Prop)


	NewManager(self, alpha, in, out, st.Ops, seqns, props, cmw)

	go func() {
		for o := range out {
			in <- o
		}
	}()

	for i := int64(3); i <= alpha+2; i++ {
		st.Ops <- store.Op{Seqn: i, Mut: store.Nop}
	}

	n := <-seqns
	w := st.Wait(n)
	props <- &Prop{n, []byte("foo")}
	e := <-w

	exp := store.Event{
		Seqn:   4,
		Path:   "/store/error",
		Body:   "bad mutation",
		Cas:    4,
		Mut:    "foo",
		Err:    os.NewError("bad mutation"),
	}

	e.Getter = nil
	assert.Equal(t, exp, e)
}
