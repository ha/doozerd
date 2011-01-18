package paxos

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestCluster(t *testing.T) {
	membership := map[string]string{"a": "x", "b": "y", "c": "z"}
	active := []string{"a", "b", "c"}
	addrs := map[string]bool{"x": true, "y": true, "z": true}
	outs := make(chanPutCloserTo)
	cx := newCluster("c", membership, active, outs)
	assert.Equal(t, 3, cx.Len(), "Len")
	assert.Equal(t, 2, cx.Quorum(), "Quorum")
	assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
	assert.Equal(t, 0, cx.indexById("a"), "indexById(\"a\")")
	assert.Equal(t, 1, cx.indexById("b"), "indexById(\"a\")")
	assert.Equal(t, 2, cx.indexById("c"), "indexById(\"a\")")
	assert.Equal(t, "a", cx.idByAddr("x"), "idByAddr(\"a\")")
	assert.Equal(t, "b", cx.idByAddr("y"), "idByAddr(\"a\")")
	assert.Equal(t, "c", cx.idByAddr("z"), "idByAddr(\"a\")")
	assert.Equal(t, 0, cx.indexByAddr("x"), "indexByAddr(\"a\")")
	assert.Equal(t, 1, cx.indexByAddr("y"), "indexByAddr(\"a\")")
	assert.Equal(t, 2, cx.indexByAddr("z"), "indexByAddr(\"a\")")
	assert.Equal(t, membership, cx.addrsById)

	m := newInvite(1)
	cx.Put(m)
	for i := 0; i < 3; i++ {
		o := <-outs
		_, ok := addrs[o.Addr]
		assert.T(t, ok)
		addrs[o.Addr] = false, false
		assert.Equal(t, m, o.M)
	}
}

func TestClusterActive(t *testing.T) {
	membership := map[string]string{"a": "x", "b": "y", "c": "z"}
	active := []string{"a"}
	outs := make(chanPutCloserTo)
	cx := newCluster("a", membership, active, outs)
	assert.Equal(t, 1, cx.Len(), "Len")
	assert.Equal(t, 1, cx.Quorum(), "Quorum")
	assert.Equal(t, 0, cx.SelfIndex(), "SelfIndex")
	assert.Equal(t, 0, cx.indexById("a"), "indexById(\"a\")")
	assert.Equal(t, -1, cx.indexById("b"), "indexById(\"a\")")
	assert.Equal(t, -1, cx.indexById("c"), "indexById(\"a\")")
	assert.Equal(t, "a", cx.idByAddr("x"), "idByAddr(\"a\")")
	assert.Equal(t, "b", cx.idByAddr("y"), "idByAddr(\"a\")")
	assert.Equal(t, "c", cx.idByAddr("z"), "idByAddr(\"a\")")
	assert.Equal(t, 0, cx.indexByAddr("x"), "indexByAddr(\"a\")")
	assert.Equal(t, -1, cx.indexByAddr("y"), "indexByAddr(\"a\")")
	assert.Equal(t, -1, cx.indexByAddr("z"), "indexByAddr(\"a\")")
	assert.Equal(t, membership, cx.addrsById)
}

func TestClusterEmpty(t *testing.T) {
	membership := map[string]string{}
	active := []string{}
	outs := make(chanPutCloserTo)
	cx := newCluster("a", membership, active, outs)
	assert.Equal(t, 0, cx.Len(), "Len")
	assert.Equal(t, 1, cx.Quorum(), "Quorum")
	assert.Equal(t, -1, cx.SelfIndex(), "SelfIndex")
	assert.Equal(t, -1, cx.indexById("a"), "indexById(\"a\")")
	assert.Equal(t, -1, cx.indexById("b"), "indexById(\"a\")")
	assert.Equal(t, -1, cx.indexById("c"), "indexById(\"a\")")
	assert.Equal(t, "", cx.idByAddr("x"), "idByAddr(\"a\")")
	assert.Equal(t, "", cx.idByAddr("y"), "idByAddr(\"a\")")
	assert.Equal(t, "", cx.idByAddr("z"), "idByAddr(\"a\")")
	assert.Equal(t, -1, cx.indexByAddr("x"), "indexByAddr(\"a\")")
	assert.Equal(t, -1, cx.indexByAddr("y"), "indexByAddr(\"a\")")
	assert.Equal(t, -1, cx.indexByAddr("z"), "indexByAddr(\"a\")")
	assert.Equal(t, membership, cx.addrsById)
}
