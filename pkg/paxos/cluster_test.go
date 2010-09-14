package paxos

import (
	"junta/assert"
	"testing"
)

func TestCluster(t *testing.T) {
	membership := map[string]string{"a": "x", "b": "y", "c": "z"}
	active := []string{"a", "b", "c"}
	cx := newCluster("c", membership, active)
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
	assert.Equal(t, []string{"x", "y", "z"}, cx.addrs(), "addrs")
	assert.Equal(t, membership, cx.addrsById)
}

func TestClusterActive(t *testing.T) {
	membership := map[string]string{"a": "x", "b": "y", "c": "z"}
	active := []string{"a"}
	cx := newCluster("a", membership, active)
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
	assert.Equal(t, []string{"x", "y", "z"}, cx.addrs(), "addrs")
	assert.Equal(t, membership, cx.addrsById)
}

func TestClusterEmpty(t *testing.T) {
	membership := map[string]string{}
	active := []string{}
	cx := newCluster("a", membership, active)
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
	assert.Equal(t, []string{}, cx.addrs(), "addrs")
	assert.Equal(t, membership, cx.addrsById)
}
