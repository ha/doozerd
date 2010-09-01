package paxos

import (
	"junta/assert"
	"junta/store"
	"testing"
)

func selfRefNewManager(start uint64, self string, nodes []string) *Manager {
	p := make(FakePutter, 1)
	st := store.New()
	rg := newRegistrar(self, st, 1)
	for i, node := range nodes {
		st.Apply(uint64(i+1), mustEncodeSet(membersKey+"/"+node, node+"addr"))
	}
	m := NewManager(start, rg, p)
	p[0] = m
	return m
}

func TestProposeAndLearn(t *testing.T) {
	exp := "foo"
	m := selfRefNewManager(1, "a", []string{"a"})

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")
}

func TestProposeAndRecv(t *testing.T) {
	exp := "foo"
	m := selfRefNewManager(1, "a", []string{"a"})

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvAltStart(t *testing.T) {
	exp := "foo"
	m := selfRefNewManager(2, "a", []string{"a"})

	got := m.Propose(exp)
	assert.Equal(t, exp, got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestProposeAndRecvMultiple(t *testing.T) {
	exp := []string{"foo", "bar"}
	seqnexp := []uint64{1, 2}
	m := selfRefNewManager(1, "a", []string{"a"})

	got0 := m.Propose(exp[0])
	assert.Equal(t, exp[0], got0, "")

	got1 := m.Propose(exp[1])
	assert.Equal(t, exp[1], got1, "")

	seqn0, v0 := m.Recv()
	assert.Equal(t, seqnexp[0], seqn0, "seqn 1")
	assert.Equal(t, exp[0], v0, "")

	seqn1, v1 := m.Recv()
	assert.Equal(t, seqnexp[1], seqn1, "seqn 2")
	assert.Equal(t, exp[1], v1, "")
}

func TestNewInstanceBecauseOfMessage(t *testing.T) {
	exp := "foo"
	m := selfRefNewManager(1, "a", []string{"a"})

	m.Put(newVoteFrom(1, 1, exp))
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestNewInstanceBecauseOfMessageTriangulate(t *testing.T) {
	exp := "bar"
	m := selfRefNewManager(1, "a", []string{"a"})

	m.Put(newVoteFrom(1, 1, exp))
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp, v, "")
}

func TestUnusedSeqn(t *testing.T) {
	exp1, exp2 := "foo", "bar"
	m := selfRefNewManager(1, "a", []string{"a"})

	m.Put(newVoteFrom(1, 1, exp1))
	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, exp1, v, "")

	got := m.Propose(exp2)
	assert.Equal(t, exp2, got, "")
	seqn, v = m.Recv()
	assert.Equal(t, uint64(2), seqn, "")
	assert.Equal(t, exp2, v, "")
}

func TestIgnoreMalformedMsg(t *testing.T) {
	m := selfRefNewManager(1, "a", []string{"a"})

	m.Put(resize(newVoteFrom(1, 1, ""), -1))

	got := m.Propose("y")
	assert.Equal(t, "y", got, "")

	seqn, v := m.Recv()
	assert.Equal(t, uint64(1), seqn, "")
	assert.Equal(t, "y", v, "")
}

func mustEncodeSet(k, v string) string {
	m, err := store.EncodeSet(k, v)
	if err != nil {
		panic(err)
	}
	return m
}

func TestReadFromStore(t *testing.T) {
	self := "a"

	// The cluster initially has 1 node (quorum of 1).
	st := store.New()
	rg := newRegistrar(self, st, 1)
	st.Apply(1, mustEncodeSet(membersKey+"/"+self, ""))

	p := make(chanPutCloser)
	m := NewManager(1, rg, p)

	// Fire up a new instance with a vote message. This instance should block
	// trying to read the list of members. If it doesn't wait, it'll
	// immediately learn the value `x`.
	in := newVoteFrom(0, 1, "x")
	in.SetSeqn(3)
	m.Put(in)

	// Satisfy the sync read of data members above. After this, there will be
	// 2 nodes in the cluster, making the quorum 2.
	st.Apply(2, mustEncodeSet(membersKey+"/b", ""))

	// Now try to make it learn a new value with 2 votes to meet the new
	// quorum.
	exp := "y"
	in = newVoteFrom(0, 2, exp)
	in.SetSeqn(3)
	m.Put(in)
	in = newVoteFrom(1, 2, exp)
	in.SetSeqn(3)
	m.Put(in)

	seqn, v := m.Recv()
	assert.Equal(t, uint64(3), seqn, "")
	assert.Equal(t, exp, v, "")
}

type putFunc func(Msg)

func (pf putFunc) Put(msg Msg) {
	go pf(msg)
}

func (pf putFunc) Close() {}

func TestManagerPutFrom(t *testing.T) {
	exp := "bar"
	seqnExp := uint64(4)
	fromAddr := "y"
	fromIndex := 1 // [a, b, c].indexof(b) => 1

	p := make(FakePutter, 1)
	st := store.New()
	rg := newRegistrar("a", st, 1)
	st.Apply(uint64(1), mustEncodeSet(membersKey+"/a", "x"))
	st.Apply(uint64(2), mustEncodeSet(membersKey+"/b", "y"))
	st.Apply(uint64(3), mustEncodeSet(membersKey+"/c", "z"))
	m := NewManager(4, rg, p)
	p[0] = m



	froms := make(chan int)

	fp := putFunc(func (msg Msg) {
		froms <- msg.From()
	})

	it := m.getInstance(seqnExp)
	it.cPutter = fp
	it.aPutter = fp
	it.lPutter = fp

	v1 := newVote(1, exp)
	v1.SetSeqn(seqnExp)
	m.PutFrom(fromAddr, v1)

	assert.Equal(t, fromIndex, <-froms, "")
	assert.Equal(t, fromIndex, <-froms, "")
	assert.Equal(t, fromIndex, <-froms, "")
}

func TestManagerAddrsFor(t *testing.T) {
	m := selfRefNewManager(2, "a", []string{"a"})
	msg := newInvite(1)
	msg.SetSeqn(1)
	assert.Equal(t, []string{"aaddr"}, m.AddrsFor(msg))
}
