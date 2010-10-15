package paxos

import (
	"junta/assert"
	"testing"
)

// Testing

type PutterFrom interface {
	PutFrom(addr string, m Msg)
}

type FakePutterFrom []PutterFrom

func (fp FakePutterFrom) PutFrom(addr string, m Msg) {
	for _, p := range fp {
		p.PutFrom(addr, m)
	}
}

type putFromWrapperTo struct {
	PutterFrom
	fromAddr string
}

func (w putFromWrapperTo) PutTo(m Msg, _ string) {
	w.PutFrom(w.fromAddr, m)
}

type clusterK cluster

func (ck *clusterK) cluster(seqn uint64) *cluster {
	return (*cluster)(ck)
}

func selfRefNewInstance(self string, nodes map[string]string) (*instance, chan result) {
	res := make(chan result, 1)
	p := make(FakePutterFrom, 1)
	cals := make([]string, len(nodes))
	i := 0
	for id := range nodes {
		cals[i] = id
		i++
	}
	cx := newCluster(self, nodes, cals, putFromWrapperTo{p, nodes[self]})
	ins := newInstance(0, (*clusterK)(cx), res)
	p[0] = ins
	return ins, res
}

func TestStartAtVote(t *testing.T) {
	ins, res := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.PutFrom("x", newVote(1, "foo"))
	assert.Equal(t, "foo", (<-res).v, "")
	ins.Close()
}

func TestStartAtLearn(t *testing.T) {
	ins, res := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.PutFrom("x", newLearn("foo"))
	assert.Equal(t, "foo", (<-res).v, "")
	ins.Close()
}

func TestLearnInEmptyCluster(t *testing.T) {
	ins, res := selfRefNewInstance("a", map[string]string{})
	ins.PutFrom("x", newLearn("foo"))
	assert.Equal(t, "foo", (<-res).v, "")
	ins.Close()
}

func TestStartAtVoteWithDuplicates(t *testing.T) {
	ins, res := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.PutFrom("x", newVote(1, "foo"))
	ins.PutFrom("x", newVote(1, "foo"))
	ins.PutFrom("x", newVote(1, "foo"))
	assert.Equal(t, "foo", (<-res).v, "")
	ins.Close()
}

func TestVoteWithQuorumOf2(t *testing.T) {
	ins, res := selfRefNewInstance("b", map[string]string{"a": "x", "b": "y", "c": "z"})
	ins.PutFrom("y", newVote(1, "foo"))
	ins.PutFrom("z", newVote(1, "foo"))
	assert.Equal(t, "foo", (<-res).v, "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins, res := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.PutFrom("x", newNominate(1, "foo"))
	ins.PutFrom("x", newNominate(1, "foo"))
	ins.PutFrom("x", newNominate(1, "foo"))
	assert.Equal(t, "foo", (<-res).v, "")
	ins.Close()
}

func TestStartAtCoord(t *testing.T) {
	ins, res := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.Propose("foo")
	assert.Equal(t, "foo", (<-res).v, "")
	ins.Close()
}

func TestMultipleInstances(t *testing.T) {
	ps := make(FakePutterFrom, 3)
	nodes := map[string]string{"a": "x", "b": "y", "c": "z"}
	cals := []string{"a", "b", "c"}
	resA := make(chan result, 1)
	cxA := newCluster("a", nodes, cals, putFromWrapperTo{ps, "x"})
	cxB := newCluster("b", nodes, cals, putFromWrapperTo{ps, "y"})
	cxC := newCluster("c", nodes, cals, putFromWrapperTo{ps, "z"})
	insA := newInstance(0, (*clusterK)(cxA), resA)
	insB := newInstance(0, (*clusterK)(cxB), make(chan result, 1))
	insC := newInstance(0, (*clusterK)(cxC), make(chan result, 1))
	ps[0] = insA
	ps[1] = insB
	ps[2] = insC

	insA.Propose("bar")
	assert.Equal(t, "bar", (<-resA).v, "")
	insA.Close()
	insB.Close()
	insC.Close()
}

func TestInstanceSendsLearn(t *testing.T) {
	ch := make(ChanPutCloserTo)
	nodes := map[string]string{"a": "x"}
	cx := newCluster("a", nodes, []string{"a"}, ch)
	it := newInstance(0, (*clusterK)(cx), make(chan result, 1))

	it.PutFrom("x", newVote(1, "foo"))

	assert.Equal(t, Packet{newLearn("foo"), "x"}, <-ch)

	it.Close()
}

//func TestDeadlock(t *testing.T) {
//	<-make(chan int)
//}
