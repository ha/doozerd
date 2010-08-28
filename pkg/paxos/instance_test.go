package paxos

import (
	"borg/assert"
	"borg/store"
	"log"
	"testing"
)

// Testing

type FakePutter []Putter

func (fp FakePutter) Put(m Msg) {
	for _, p := range fp {
		p.Put(m)
	}
}

func selfRefNewInstance(self string, nodes []string, logger *log.Logger) *instance {
	p := make([]Putter, 1)

	st := store.New(logger)
	for i, node := range nodes {
		st.Apply(uint64(i+1), mustEncodeSet("/b/borg/members/" + node, ""))
	}

	ins := newInstance(self, st, 0, FakePutter(p), logger)
	p[0] = ins
	return ins
}

func TestStartAtLearn(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtLearnWithDuplicates(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestLearnWithQuorumOf2(t *testing.T) {
	ins := selfRefNewInstance("b", []string{"a", "b", "c"}, logger)
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestValueCanBeCalledMoreThanOnce(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(newNominateFrom(1, 1, "foo"))
	ins.Put(newNominateFrom(1, 1, "foo"))
	ins.Put(newNominateFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtCoord(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestMultipleInstances(t *testing.T) {
	ps := make([]Putter, 3)
	nodes := []string{"a", "b", "c"}
	st := store.New(logger)
	for i, node := range nodes {
		st.Apply(uint64(i+1), mustEncodeSet("/b/borg/members/" + node, ""))
	}
	insA := newInstance("a", st, 0, putWrapper{1, 3, FakePutter(ps)}, logger)
	insB := newInstance("b", st, 0, putWrapper{1, 1, FakePutter(ps)}, logger)
	insC := newInstance("c", st, 0, putWrapper{1, 2, FakePutter(ps)}, logger)
	ps[0] = insA
	ps[1] = insB
	ps[2] = insC

	insA.Propose("bar")
	assert.Equal(t, "bar", insA.Value(), "")
	insA.Close()
	insB.Close()
	insC.Close()
}

//func TestDeadlock(t *testing.T) {
//	<-make(chan int)
//}
