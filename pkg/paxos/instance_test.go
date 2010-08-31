package paxos

import (
	"junta/assert"
	"testing"
)

// Testing

type FakePutter []Putter

func (fp FakePutter) Put(m Msg) {
	for _, p := range fp {
		p.Put(m)
	}
}

type putFromWrapper struct {
	from int
	Putter
}

func (w putFromWrapper) Put(m Msg) {
	m.SetFrom(w.from)
	w.Putter.Put(m)
}

func selfRefNewInstance(self string, nodes []string) *instance {
	p := make([]Putter, 1)
	cx := newCluster(self, nodes)
	ins := newInstance(func() *cluster { return cx }, FakePutter(p))
	p[0] = ins
	return ins
}

func TestStartAtLearn(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtLearnWithDuplicates(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestLearnWithQuorumOf2(t *testing.T) {
	ins := selfRefNewInstance("b", []string{"a", "b", "c"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestValueCanBeCalledMoreThanOnce(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"})
	ins.Put(newNominateFrom(1, 1, "foo"))
	ins.Put(newNominateFrom(1, 1, "foo"))
	ins.Put(newNominateFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtCoord(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"})
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestMultipleInstances(t *testing.T) {
	ps := make([]Putter, 3)
	nodes := []string{"a", "b", "c"}
	cxA := func() *cluster { return newCluster("a", nodes) }
	cxB := func() *cluster { return newCluster("a", nodes) }
	cxC := func() *cluster { return newCluster("a", nodes) }
	insA := newInstance(cxA, putFromWrapper{3, FakePutter(ps)})
	insB := newInstance(cxB, putFromWrapper{1, FakePutter(ps)})
	insC := newInstance(cxC, putFromWrapper{2, FakePutter(ps)})
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
