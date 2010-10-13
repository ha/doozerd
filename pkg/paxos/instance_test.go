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

func selfRefNewInstance(self string, nodes map[string]string) *instance {
	p := make(FakePutterFrom, 1)
	cals := make([]string, len(nodes))
	i := 0
	for id := range nodes {
		cals[i] = id
		i++
	}
	cx := newCluster(self, nodes, cals, putFromWrapperTo{p, nodes[self]})
	ins := newInstance()
	ins.setCluster(cx)
	p[0] = ins
	return ins
}

func TestStartAtVote(t *testing.T) {
	ins := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtLearn(t *testing.T) {
	ins := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.Put(newLearn("foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestLearnInEmptyCluster(t *testing.T) {
	ins := selfRefNewInstance("a", map[string]string{})
	ins.Put(newLearn("foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtVoteWithDuplicates(t *testing.T) {
	ins := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestVoteWithQuorumOf2(t *testing.T) {
	ins := selfRefNewInstance("b", map[string]string{"a": "x", "b": "y", "c": "z"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	ins.Put(newVoteFrom(2, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestValueCanBeCalledMoreThanOnce(t *testing.T) {
	ins := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.Put(newVoteFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtAccept(t *testing.T) {
	ins := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.Put(newNominateFrom(1, 1, "foo"))
	ins.Put(newNominateFrom(1, 1, "foo"))
	ins.Put(newNominateFrom(1, 1, "foo"))
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestStartAtCoord(t *testing.T) {
	ins := selfRefNewInstance("a", map[string]string{"a": "x"})
	ins.Propose("foo")
	assert.Equal(t, "foo", ins.Value(), "")
	ins.Close()
}

func TestMultipleInstances(t *testing.T) {
	ps := make(FakePutterFrom, 3)
	nodes := map[string]string{"a": "x", "b": "y", "c": "z"}
	cals := []string{"a", "b", "c"}
	insA := newInstance()
	insB := newInstance()
	insC := newInstance()
	ps[0] = insA
	ps[1] = insB
	ps[2] = insC
	insA.setCluster(newCluster("a", nodes, cals, putFromWrapperTo{ps, "x"}))
	insB.setCluster(newCluster("b", nodes, cals, putFromWrapperTo{ps, "y"}))
	insC.setCluster(newCluster("c", nodes, cals, putFromWrapperTo{ps, "z"}))

	insA.Propose("bar")
	assert.Equal(t, "bar", insA.Value(), "")
	insA.Close()
	insB.Close()
	insC.Close()
}

func TestInstanceCluster(t *testing.T) {
	ch := make(chan *cluster)
	nodes := map[string]string{"a": "x"}
	p := make(FakePutterFrom, 1)
	cx := newCluster("a", nodes, []string{"a"}, putFromWrapperTo{p, "x"})
	it := newInstance()
	it.setCluster(cx)
	p[0] = it

	go func() {
		ch <- it.cluster()
	}()
	it.Propose("foo")
	assert.Equal(t, cx, <-ch, "")
	it.Close()
}

func TestInstanceSendsLearn(t *testing.T) {
	ch := make(ChanPutCloserTo)
	nodes := map[string]string{"a": "x"}
	cx := newCluster("a", nodes, []string{"a"}, ch)
	it := newInstance()
	it.setCluster(cx)

	it.Put(newVoteFrom(0, 1, "foo"))

	assert.Equal(t, Packet{newLearn("foo"), "x"}, <-ch)

	it.Close()
}

//func TestDeadlock(t *testing.T) {
//	<-make(chan int)
//}
