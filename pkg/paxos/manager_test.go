package paxos

import (
	"fmt"
	"junta/assert"
	"junta/store"
	"strconv"
	"testing"
)

func selfRefNewManager(self string, alpha int) (*Manager, *store.Store) {
	p := make(FakePutterFrom, 1)
	st := store.New()
	st.Apply(1, mustEncodeSet(membersDir+"a", "x"))
	st.Apply(2, mustEncodeSet(slotDir+"0", "a"))
	m := NewManager(self, 2, alpha, st, putFromWrapperTo{p, "x"})
	p[0] = m
	return m, st
}

func mutualRefManagers(n, alpha int) ([]*Manager, *store.Store) {
	st := store.New()
	p := make(FakePutterFrom, n)
	ms := make([]*Manager, n)
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("addr%d", i)
		id := fmt.Sprintf("id%d", i)
		st.Apply(uint64(2*i+1), mustEncodeSet(membersDir+id, addr))
		st.Apply(uint64(2*i+2), mustEncodeSet(slotDir+strconv.Itoa(i), id))
		ms[i] = NewManager(id, uint64(2*n), alpha, st, putFromWrapperTo{p, addr})
		p[i] = ms[i]
	}
	for s := uint64(2*n + 1); s < uint64(2*n+alpha); s++ {
		st.Apply(s, store.Nop)
	}
	return ms, st
}

func TestProposeAndLearn(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager("a", 1)

	ix := m.getInstance(<-m.seqns)
	ix.Propose(exp)
	got := (<-m.learned).v

	assert.Equal(t, exp, got, "")
}

func TestProposeAndRecv(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager("a", 1)

	ix := m.getInstance(<-m.seqns)
	ix.Propose(exp)

	seqn, v := m.Recv()
	assert.Equal(t, uint64(3), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvAltStart(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager("a", 1)

	ix := m.getInstance(<-m.seqns)
	ix.Propose(exp)

	seqn, v := m.Recv()
	assert.Equal(t, uint64(3), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvMultiple(t *testing.T) {
	exp := []string{"/foo", "/bar"}
	seqnexp := []uint64{3, 4}
	m, st := selfRefNewManager("a", 1)

	ix := m.getInstance(<-m.seqns)
	ix.Propose(exp[0])

	seqn0, v0 := m.Recv()
	assert.Equal(t, seqnexp[0], seqn0, "seqn 1")
	assert.Equal(t, exp[0], v0, "")

	st.Apply(seqn0, v0)

	ix = m.getInstance(<-m.seqns)
	ix.Propose(exp[1])

	seqn1, v1 := m.Recv()
	assert.Equal(t, seqnexp[1], seqn1, "seqn 1")
	assert.Equal(t, exp[1], v1, "")
}

func TestProposeAndRecvFill(t *testing.T) {
	ms, st := mutualRefManagers(2, 10)

	mut1 := store.MustEncodeSet("/foo", "a", store.Clobber)
	mut2 := store.MustEncodeSet("/bar", "b", store.Clobber)

	ch14 := st.Wait(14)
	ch15 := st.Wait(15)
	ch16 := st.Wait(16)

	go func() {
		for {
			st.Apply(ms[0].Recv())
		}
	}()
	go ms[0].Propose(mut1)
	go ms[0].Propose(mut2)

	assert.Equal(t, mut1, (<-ch14).Mut)
	assert.Equal(t, store.Nop, (<-ch15).Mut)
	assert.Equal(t, mut2, (<-ch16).Mut)
}

func TestNewInstanceBecauseOfMessage(t *testing.T) {
	exp := "foo"
	m, _ := selfRefNewManager("a", 1)

	msg := newVote(1, exp)
	msg.SetSeqn(1)
	m.PutFrom(m.Self+"addr", msg)
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestNewInstanceBecauseOfMessageTriangulate(t *testing.T) {
	exp := "bar"
	m, _ := selfRefNewManager("a", 1)

	msg := newVote(1, exp)
	msg.SetSeqn(1)
	m.PutFrom(m.Self+"addr", msg)
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestUnusedSeqn(t *testing.T) {
	exp1, exp2 := "foo", "bar"
	m, _ := selfRefNewManager("a", 1)

	msg := newVote(1, exp1)
	msg.SetSeqn(1)
	m.PutFrom(m.Self+"addr", msg)
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp1, v, "")

	ix := m.getInstance(<-m.seqns)
	ix.Propose(exp2)
	seqn, v = m.Recv()
	assert.Equal(t, uint64(3), seqn, "")
	assert.Equal(t, exp2, v, "")
}

func TestIgnoreMalformedMsg(t *testing.T) {
	m, _ := selfRefNewManager("a", 1)

	m.PutFrom(m.Self+"addr", resize(newVote(1, ""), -1))

	ix := m.getInstance(<-m.seqns)
	ix.Propose("y")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(3), seqn, "")
	assert.Equal(t, "y", v, "")
}

func TestProposeAndStore(t *testing.T) {
	exp := "foo"
	mg, st := selfRefNewManager("a", 1)

	go func() {
		for {
			st.Apply(mg.Recv())
		}
	}()

	ch := st.Wait(3)
	mg.Propose(exp)
	assert.Equal(t, exp, (<-ch).Mut)
}

func TestProposeBadMutation(t *testing.T) {
	mg, st := selfRefNewManager("a", 1)

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

func play(st *store.Store) {
	st.Apply(3, mustEncodeSet(membersDir+"b", "y"))
	st.Apply(4, mustEncodeSet(slotDir+"1", "b"))
	st.Apply(5, mustEncodeSet(membersDir+"1", "s"))
	st.Apply(6, mustEncodeSet(slotDir+"2", "1"))
	st.Apply(7, mustEncodeSet(membersDir+"c", "z"))
	st.Apply(8, mustEncodeSet(slotDir+"3", "c"))
	st.Apply(9, mustEncodeSet(membersDir+"0", "t"))
	st.Apply(10, mustEncodeSet(slotDir+"4", "0"))
	st.Apply(11, mustEncodeSet(membersDir+"d", "w"))
	st.Apply(12, mustEncodeSet(slotDir+"5", "d"))
	st.Apply(13, store.Nop)
	st.Apply(14, store.Nop)
	st.Apply(15, store.Nop)
	st.Apply(16, store.Nop)
	st.Apply(17, store.Nop)
	st.Apply(18, store.Nop)
}

func TestManagerGetSeqnsA(t *testing.T) {
	m, st := selfRefNewManager("a", 5)
	play(st)

	assert.Equal(t, uint64(7), <-m.seqns)
	assert.Equal(t, uint64(8), <-m.seqns)
	assert.Equal(t, uint64(10), <-m.seqns)
	assert.Equal(t, uint64(13), <-m.seqns)
	assert.Equal(t, uint64(20), <-m.seqns)
}

func TestManagerGetSeqnsB(t *testing.T) {
	m, st := selfRefNewManager("b", 5)
	play(st)

	assert.Equal(t, uint64(9), <-m.seqns)
	assert.Equal(t, uint64(11), <-m.seqns)
	assert.Equal(t, uint64(14), <-m.seqns)
	assert.Equal(t, uint64(21), <-m.seqns)
}

func TestManagerGetSeqns1(t *testing.T) {
	m, st := selfRefNewManager("1", 5)
	play(st)

	assert.Equal(t, uint64(12), <-m.seqns)
	assert.Equal(t, uint64(16), <-m.seqns)
	assert.Equal(t, uint64(19), <-m.seqns)
}

func TestManagerGetSeqnsC(t *testing.T) {
	m, st := selfRefNewManager("c", 5)
	play(st)

	assert.Equal(t, uint64(22), <-m.seqns)
}

func TestManagerGetSeqns0(t *testing.T) {
	m, st := selfRefNewManager("0", 5)
	play(st)

	assert.Equal(t, uint64(15), <-m.seqns)
	assert.Equal(t, uint64(18), <-m.seqns)
}

func TestManagerGetSeqnsD(t *testing.T) {
	m, st := selfRefNewManager("d", 5)
	play(st)

	assert.Equal(t, uint64(17), <-m.seqns)
	assert.Equal(t, uint64(23), <-m.seqns)
}
