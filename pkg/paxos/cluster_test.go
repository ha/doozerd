package paxos

import (
	"doozer/assert"
	"testing"
)

func TestCluster(t *testing.T) {
	membership := map[string]string{"a": "x", "b": "y", "c": "z"}
	active := []string{"a", "b", "c"}
	outs := make(ChanPutCloserTo)
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
	assert.Equal(t, Packet{m, "x"}, <-outs)
	assert.Equal(t, Packet{m, "y"}, <-outs)
	assert.Equal(t, Packet{m, "z"}, <-outs)
}

func TestClusterActive(t *testing.T) {
	membership := map[string]string{"a": "x", "b": "y", "c": "z"}
	active := []string{"a"}
	outs := make(ChanPutCloserTo)
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
	outs := make(ChanPutCloserTo)
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
