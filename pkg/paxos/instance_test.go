package paxos

import (
	"junta/assert"
	"testing"
)

// Testing

type PutPutterTo struct {
	Putter
}

func (p PutPutterTo) PutTo(m Msg, addr string) {
	p.Put(m)
}

type FakePutterTo []PutterTo

func (fp FakePutterTo) PutTo(m Msg, addr string) {
	for _, p := range fp {
		p.PutTo(m, addr)
	}
}

func selfRefNewInstance(self string, nodes map[string]string) *instance {
	p := make(FakePutterTo, 1)
	cals := make([]string, len(nodes))
	i := 0
	for id := range nodes {
		cals[i] = id
		i++
	}
	cx := newCluster(self, nodes, cals, p)
	ins := newInstance()
	ins.setCluster(cx)
	p[0] = PutPutterTo{ins}
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

type putFromWrapperTo struct {
	from int
	PutterTo
}

func (w putFromWrapperTo) PutTo(m Msg, addr string) {
	m.SetFrom(w.from)
	w.PutterTo.PutTo(m, addr)
}

func TestMultipleInstances(t *testing.T) {
	ps := make(FakePutterTo, 3)
	nodes := map[string]string{"a": "x", "b": "y", "c": "z"}
	cals := []string{"a", "b", "c"}
	insA := newInstance()
	insB := newInstance()
	insC := newInstance()
	ps[0] = PutPutterTo{insA}
	ps[1] = PutPutterTo{insB}
	ps[2] = PutPutterTo{insC}
	insA.setCluster(newCluster("a", nodes, cals, putFromWrapperTo{3, ps}))
	insB.setCluster(newCluster("b", nodes, cals, putFromWrapperTo{1, ps}))
	insC.setCluster(newCluster("c", nodes, cals, putFromWrapperTo{2, ps}))

	insA.Propose("bar")
	assert.Equal(t, "bar", insA.Value(), "")
	insA.Close()
	insB.Close()
	insC.Close()
}

func TestInstanceCluster(t *testing.T) {
	ch := make(chan *cluster)
	nodes := map[string]string{"a": "x"}
	p := make(FakePutterTo, 1)
	cx := newCluster("a", nodes, []string{"a"}, p)
	it := newInstance()
	it.setCluster(cx)
	p[0] = PutPutterTo{it}

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
	p := make(FakePutterTo, 2)
	cx := newCluster("a", nodes, []string{"a"}, p)
	it := newInstance()
	it.setCluster(cx)
	p[0] = PutPutterTo{it}
	p[1] = ch

	it.Put(newVoteFrom(0, 1, "foo"))

	assert.Equal(t, Packet{newLearn("foo"), "x"}, <-ch)

	it.Close()
}

//func TestDeadlock(t *testing.T) {
//	<-make(chan int)
//}
