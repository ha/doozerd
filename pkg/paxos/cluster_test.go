package paxos

import (
	"junta/assert"
	"testing"
)

func TestCluster(t *testing.T) {
	cx := newCluster("c", map[string]string{"a":"x", "b":"y", "c":"z"})
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
}

func TestClusterNil(t *testing.T) {
	cx := newCluster("c", map[string]string{"a":"x", "b":"y", "c":"z", "":""})
	assert.Equal(t, 3, cx.Len(), "Len")
	assert.Equal(t, 2, cx.Quorum(), "Quorum")
	assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
}
