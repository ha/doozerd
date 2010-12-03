package paxos

import (
	"github.com/bmizerany/assert"
	"doozer/store"
	"testing"
)

func fw(t chan<- store.Op, s <-chan store.Op) {
	for o := range s {
		t <- o
	}
}

func selfRefNewManager(self string, alpha int) (*Manager, *store.Store, chan store.Op) {
	ops := make(chan store.Op)
	p := make(FakePutterFrom, 1)
	st := store.New()
	st.Ops <- store.Op{1, mustEncodeSet(membersDir+"a", "x")}
	st.Ops <- store.Op{2, mustEncodeSet(slotDir+"0", "a")}
	m := NewManager(self, 2, alpha, st, ops, putFromWrapperTo{p, "x"})
	p[0] = m
	return m, st, ops
}

func TestProposeAndLearn(t *testing.T) {
	exp := "foo"
	m, _, ops := selfRefNewManager("a", 1)

	seqn := <-m.seqns
	ix := m.getInstance(seqn)
	ix.Propose(exp)

	got := <-ops
	assert.Equal(t, uint64(3), got.Seqn)
	assert.Equal(t, exp, got.Mut)
}

func TestProposeAndLearnMultiple(t *testing.T) {
	exp := []string{"/foo", "/bar"}
	seqnexp := []uint64{3, 4}
	m, st, ops := selfRefNewManager("a", 1)

	ix := m.getInstance(<-m.seqns)
	ix.Propose(exp[0])

	got0 := <-ops
	assert.Equal(t, seqnexp[0], got0.Seqn, "seqn 1")
	assert.Equal(t, exp[0], got0.Mut, "")

	st.Ops <- store.Op{got0.Seqn, got0.Mut}

	ix = m.getInstance(<-m.seqns)
	ix.Propose(exp[1])

	got1 := <-ops
	assert.Equal(t, seqnexp[1], got1.Seqn, "seqn 1")
	assert.Equal(t, exp[1], got1.Mut, "")
}

func TestManagerFill(t *testing.T) {
	st := store.New()
	p := make(ChanPutCloserTo)
	st.Ops <- store.Op{1, mustEncodeSet(membersDir+"a", "x")}
	st.Ops <- store.Op{2, mustEncodeSet(slotDir+"0", "a")}
	mg := NewManager("a", 2, 1, st, st.Ops, p)

	mg.fillUntil <- 4
	assert.Equal(t, uint64(3), (<-p).Msg.Seqn())
}

func TestNewInstanceBecauseOfMessage(t *testing.T) {
	exp := "foo"
	m, _, ops := selfRefNewManager("a", 1)

	msg := newVote(3, exp)
	msg.SetSeqn(3)
	m.PutFrom(m.Self+"addr", msg)
	got := <-ops
	assert.Equal(t, uint64(3), got.Seqn)
	assert.Equal(t, exp, got.Mut)
}

func TestNewInstanceBecauseOfMessageTriangulate(t *testing.T) {
	exp := "bar"
	m, _, ops := selfRefNewManager("a", 1)

	msg := newVote(3, exp)
	msg.SetSeqn(3)
	m.PutFrom(m.Self+"addr", msg)
	got := <-ops
	assert.Equal(t, uint64(3), got.Seqn)
	assert.Equal(t, exp, got.Mut)
}

func TestUnusedSeqn(t *testing.T) {
	exp := "bar"
	m, _, ops := selfRefNewManager("a", 1)

	ix := m.getInstance(<-m.seqns)
	ix.Propose(exp)
	got := <-ops
	assert.Equal(t, uint64(3), got.Seqn)
	assert.Equal(t, exp, got.Mut)
}

func TestIgnoreMalformedMsg(t *testing.T) {
	m, _, ops := selfRefNewManager("a", 1)

	m.PutFrom(m.Self+"addr", resize(newVote(1, ""), -1))

	ix := m.getInstance(<-m.seqns)
	ix.Propose("y")

	got := <-ops
	assert.Equal(t, uint64(3), got.Seqn)
	assert.Equal(t, "y", got.Mut)
}

func TestProposeAndStore(t *testing.T) {
	exp := "foo"
	mg, st, ops := selfRefNewManager("a", 1)
	go fw(st.Ops, ops)

	ch := st.Wait(3)
	mg.Propose(exp)
	assert.Equal(t, exp, (<-ch).Mut)
}

func BenchmarkPropose(b *testing.B) {
	mg, st, ops := selfRefNewManager("a", 1)
	go fw(st.Ops, ops)

	for i := 0; i < b.N; i++ {
		mg.Propose("foo")
	}
}

func TestProposeBadMutation(t *testing.T) {
	mg, st, ops := selfRefNewManager("a", 1)
	go fw(st.Ops, ops)

	_, _, err := mg.Propose("foo")
	assert.Equal(t, store.ErrBadMutation, err)
}

func mustEncodeSet(k, v string) string {
	m, err := store.EncodeSet(k, v, store.Clobber)
	if err != nil {
		panic(err)
	}
	return m
}

func TestReadFromStore(t *testing.T) {
	// The cluster initially has 1 node (quorum of 1).
	st := store.New()
	ops := make(chan store.Op)
	p := make(ChanPutCloserTo)
	self := "a"
	addr := "x"
	st.Ops <- store.Op{1, mustEncodeSet(membersDir+self, addr)}
	st.Ops <- store.Op{2, mustEncodeSet(slotDir+"0", self)}
	m := NewManager(self, 2, 1, st, ops, p)

	// Fire up a new instance with a vote message. This instance should block
	// trying to read the list of members. If it doesn't wait, it'll
	// immediately learn the value `x`.
	in := newVote(1, "x")
	in.SetSeqn(5)
	go m.PutFrom(addr, in)

	// Satisfy the sync read of data members above. After this, there will be
	// 2 nodes in the cluster, making the quorum 2.
	bAddr := "y"
	st.Ops <- store.Op{3, mustEncodeSet(membersDir+"b", bAddr)}
	st.Ops <- store.Op{4, mustEncodeSet(slotDir+"1", "b")}

	// Now try to make it learn a new value with 2 votes to meet the new
	// quorum.
	exp := "y"
	in = newVote(2, exp)
	in.SetSeqn(5)
	m.PutFrom(addr, in)
	in = newVote(2, exp)
	in.SetSeqn(5)
	m.PutFrom(bAddr, in)

	got := <-ops
	assert.Equal(t, uint64(5), got.Seqn)
	assert.Equal(t, exp, got.Mut)
}

func play(st *store.Store) {
	st.Ops <- store.Op{3, mustEncodeSet(membersDir+"b", "y")}
	st.Ops <- store.Op{4, mustEncodeSet(slotDir+"1", "b")}
	st.Ops <- store.Op{5, mustEncodeSet(membersDir+"1", "s")}
	st.Ops <- store.Op{6, mustEncodeSet(slotDir+"2", "1")}
	st.Ops <- store.Op{7, mustEncodeSet(membersDir+"c", "z")}
	st.Ops <- store.Op{8, mustEncodeSet(slotDir+"3", "c")}
	st.Ops <- store.Op{9, mustEncodeSet(membersDir+"0", "t")}
	st.Ops <- store.Op{10, mustEncodeSet(slotDir+"4", "0")}
	st.Ops <- store.Op{11, mustEncodeSet(membersDir+"d", "w")}
	st.Ops <- store.Op{12, mustEncodeSet(slotDir+"5", "d")}
	st.Ops <- store.Op{13, store.Nop}
	st.Ops <- store.Op{14, store.Nop}
	st.Ops <- store.Op{15, store.Nop}
	st.Ops <- store.Op{16, store.Nop}
	st.Ops <- store.Op{17, store.Nop}
	st.Ops <- store.Op{18, store.Nop}
}

func TestManagerGetSeqnsA(t *testing.T) {
	m, st, ops := selfRefNewManager("a", 5)
	go fw(st.Ops, ops)
	play(st)

	assert.Equal(t, uint64(7), <-m.seqns)
	assert.Equal(t, uint64(8), <-m.seqns)
	assert.Equal(t, uint64(10), <-m.seqns)
	assert.Equal(t, uint64(13), <-m.seqns)
	assert.Equal(t, uint64(20), <-m.seqns)
}

func TestManagerGetSeqnsB(t *testing.T) {
	m, st, ops := selfRefNewManager("b", 5)
	go fw(st.Ops, ops)
	play(st)

	assert.Equal(t, uint64(9), <-m.seqns)
	assert.Equal(t, uint64(11), <-m.seqns)
	assert.Equal(t, uint64(14), <-m.seqns)
	assert.Equal(t, uint64(21), <-m.seqns)
}

func TestManagerGetSeqns1(t *testing.T) {
	m, st, ops := selfRefNewManager("1", 5)
	go fw(st.Ops, ops)
	play(st)

	assert.Equal(t, uint64(12), <-m.seqns)
	assert.Equal(t, uint64(16), <-m.seqns)
	assert.Equal(t, uint64(19), <-m.seqns)
}

func TestManagerGetSeqnsC(t *testing.T) {
	m, st, ops := selfRefNewManager("c", 5)
	go fw(st.Ops, ops)
	play(st)

	assert.Equal(t, uint64(22), <-m.seqns)
}

func TestManagerGetSeqns0(t *testing.T) {
	m, st, ops := selfRefNewManager("0", 5)
	go fw(st.Ops, ops)
	play(st)

	assert.Equal(t, uint64(15), <-m.seqns)
	assert.Equal(t, uint64(18), <-m.seqns)
}

func TestManagerGetSeqnsD(t *testing.T) {
	m, st, ops := selfRefNewManager("d", 5)
	go fw(st.Ops, ops)
	play(st)

	assert.Equal(t, uint64(17), <-m.seqns)
	assert.Equal(t, uint64(23), <-m.seqns)
}

func TestManagerAppliedShutdown(t *testing.T) {
	st := store.New()
	st.Ops <- store.Op{1, mustEncodeSet(membersDir+"a", "x")}
	st.Ops <- store.Op{2, mustEncodeSet(slotDir+"0", "a")}
	mg := NewManager("a", 2, 1, st, st.Ops, nil)

	assert.NotEqual(t, instance(nil), mg.getInstance(3))

	st.Ops <- store.Op{3, store.Nop}
	<-st.Seqns // give mg a chance to get the store.Event for seqn 3

	assert.Equal(t, instance(nil), mg.getInstance(3))
}

func TestManagerAppliedNeverStarted(t *testing.T) {
	st := store.New()
	st.Ops <- store.Op{1, mustEncodeSet(membersDir+"a", "x")}
	st.Ops <- store.Op{2, mustEncodeSet(slotDir+"0", "a")}
	mg := NewManager("a", 2, 1, st, st.Ops, nil)

	st.Ops <- store.Op{3, store.Nop}
	<-st.Seqns // give mg a chance to get the store.Event for seqn 3

	assert.Equal(t, instance(nil), mg.getInstance(3))
}

func TestManagerReply(t *testing.T) {
	st := store.New()
	st.Ops <- store.Op{1, mustEncodeSet(membersDir+"a", "x")}
	st.Ops <- store.Op{2, mustEncodeSet(slotDir+"0", "a")}
	ch := make(ChanPutCloserTo)
	mg := NewManager("a", 2, 1, st, st.Ops, ch)

	mut := store.MustEncodeSet("/foo", "bar", store.Clobber)
	st.Ops <- store.Op{3, mut}
	<-st.Seqns // give mg a chance to get the store.Event for seqn 3
	msg := newInvite(1)
	msg.SetSeqn(3)
	it := mg.getInstance(3)
	assert.Equal(t, instance(nil), it)
	mg.PutFrom("x", msg)
	exp := newLearn(mut)
	exp.SetSeqn(3)
	assert.Equal(t, Packet{exp, "x"}, <-ch)
}

func TestManagerClosesInstance(t *testing.T) {
	st := store.New()
	st.Ops <- store.Op{1, mustEncodeSet(membersDir+"a", "x")}
	st.Ops <- store.Op{2, mustEncodeSet(slotDir+"0", "a")}
	mg := NewManager("a", 2, 1, st, st.Ops, nil)

	it := mg.getInstance(3)
	st.Ops <- store.Op{3, store.Nop}
	<-it
	assert.T(t, closed(it))
}
