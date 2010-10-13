package paxos

import (
	"junta/assert"
	"junta/store"
	"testing"
)

func selfRefNewManager() (*Manager, *store.Store) {
	p := make(FakePutterFrom, 1)
	st := store.New()
	self := "a"
	st.Apply(uint64(1), mustEncodeSet(membersKey+"/"+self, self+"addr"))
	m := NewManager(self, uint64(1), 1, st, putFromWrapperTo{p, self+"addr"})
	p[0] = m
	return m, st
}

func TestProposeAndLearn(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager()

	_, ix := m.getInstance(<-m.seqns)
	ix.Propose(exp)
	got := ix.Value()

	assert.Equal(t, exp, got, "")
}

func TestProposeAndRecv(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager()

	_, ix := m.getInstance(<-m.seqns)
	ix.Propose(exp)
	got := ix.Value()
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvAltStart(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager()

	_, ix := m.getInstance(<-m.seqns)
	ix.Propose(exp)
	got := ix.Value()
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvMultiple(t *testing.T) {
	exp := []string{"/foo", "/bar"}
	seqnexp := []uint64{2, 3}
	m, st := selfRefNewManager()

	_, ix := m.getInstance(<-m.seqns)
	ix.Propose(exp[0])
	got0 := ix.Value()
	assert.Equal(t, exp[0], got0, "")

	seqn0, v0 := m.Recv()
	assert.Equal(t, seqnexp[0], seqn0, "seqn 1")
	assert.Equal(t, exp[0], v0, "")

	st.Apply(seqn0, v0)

	_, ix = m.getInstance(<-m.seqns)
	ix.Propose(exp[1])
	got1 := ix.Value()
	assert.Equal(t, exp[1], got1, "")

	seqn1, v1 := m.Recv()
	assert.Equal(t, seqnexp[1], seqn1, "seqn 1")
	assert.Equal(t, exp[1], v1, "")
}

func TestNewInstanceBecauseOfMessage(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager()

	msg := newVote(1, exp)
	msg.SetSeqn(1)
	m.PutFrom(m.Self+"addr", msg)
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestNewInstanceBecauseOfMessageTriangulate(t *testing.T) {
	exp := "bar"
	m, _ := selfRefNewManager()

	msg := newVote(1, exp)
	msg.SetSeqn(1)
	m.PutFrom(m.Self+"addr", msg)
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestUnusedSeqn(t *testing.T) {
	exp1, exp2 := "foo", "bar"
	m, _ := selfRefNewManager()

	msg := newVote(1, exp1)
	msg.SetSeqn(1)
	m.PutFrom(m.Self+"addr", msg)
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp1, v, "")

	_, ix := m.getInstance(<-m.seqns)
	ix.Propose(exp2)
	got := ix.Value()
	assert.Equal(t, exp2, got, "")
	seqn, v = m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, exp2, v, "")
}

func TestIgnoreMalformedMsg(t *testing.T) {
	m, _ := selfRefNewManager()

	m.PutFrom(m.Self+"addr", resize(newVote(1, ""), -1))

	_, ix := m.getInstance(<-m.seqns)
	ix.Propose("y")
	got := ix.Value()
	assert.Equal(t, "y", got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, "y", v, "")
}

func TestProposeAndStore(t *testing.T) {
	exp := "foo"
	mg, st := selfRefNewManager()

	go func() {
		for {
			st.Apply(mg.Recv())
		}
	}()

	_, got, _ := mg.Propose(exp)
	assert.Equal(t, exp, got, "")
}

func TestProposeBadMutation(t *testing.T) {
	mg, st := selfRefNewManager()

	go func() {
		for {
			st.Apply(mg.Recv())
		}
	}()

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
	p := make(ChanPutCloserTo)
	self := "a"
	addr := "x"
	st.Apply(1, mustEncodeSet(membersDir+self, addr))
	st.Apply(2, mustEncodeSet(slotDir+"0", self))
	m := NewManager(self, 2, 1, st, p)

	// Fire up a new instance with a vote message. This instance should block
	// trying to read the list of members. If it doesn't wait, it'll
	// immediately learn the value `x`.
	in := newVote(1, "x")
	in.SetSeqn(5)
	go m.PutFrom(addr, in)

	// Satisfy the sync read of data members above. After this, there will be
	// 2 nodes in the cluster, making the quorum 2.
	bAddr := "y"
	st.Apply(3, mustEncodeSet(membersDir+"b", bAddr))
	st.Apply(4, mustEncodeSet(slotDir+"1", "b"))

	// Now try to make it learn a new value with 2 votes to meet the new
	// quorum.
	exp := "y"
	in = newVote(2, exp)
	in.SetSeqn(5)
	m.PutFrom(addr, in)
	in = newVote(2, exp)
	in.SetSeqn(5)
	m.PutFrom(bAddr, in)

	seqn, v := m.Recv()
	assert.Equal(t, uint64(5), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestManagerGetInstanceForPropose(t *testing.T) {
	m, _ := selfRefNewManager()
	seqn, _ := m.getInstance(<-m.seqns)
	assert.Equal(t, uint64(2), seqn)
}
