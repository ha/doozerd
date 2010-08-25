package paxos

import (
	"borg/assert"
	"log"
	"testing"
)

// Testing

type FakePutter []Putter

func (fp FakePutter) Put(m Message) {
	for _, p := range fp {
		p.Put(m)
	}
}

func selfRefNewInstance(self string, nodes []string, logger *log.Logger) *Instance {
	p := make([]Putter, 1)
	cx := NewCluster(self, nodes, FakePutter(p))
	ins := NewInstance(cx, logger)
	p[0] = ins
	return ins
}

func TestStartAtLearn(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtLearnWithDuplicates(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestLearnWithQuorumOf2(t *testing.T) {
	ins := selfRefNewInstance("b", []string{"a", "b", "c"}, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	ins.Put(m("2:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestValueCanBeCalledMoreThanOnce(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(m("1:*:VOTE:1:foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins := selfRefNewInstance("a", []string{"a"}, logger)
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
	ins.Put(m("1:*:NOMINATE:1:foo"))
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
	cxA := NewCluster("a", nodes, PutWrapper{1, 3, FakePutter(ps)})
	cxB := NewCluster("a", nodes, PutWrapper{1, 1, FakePutter(ps)})
	cxC := NewCluster("a", nodes, PutWrapper{1, 2, FakePutter(ps)})
	insA := NewInstance(cxA, logger)
	insB := NewInstance(cxB, logger)
	insC := NewInstance(cxC, logger)
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
