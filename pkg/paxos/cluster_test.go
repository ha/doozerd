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
	assert.Equal(t, 0, cx.indexOf("a"), "indexOf(\"a\")")
	assert.Equal(t, 1, cx.indexOf("b"), "indexOf(\"a\")")
	assert.Equal(t, 2, cx.indexOf("c"), "indexOf(\"a\")")
	assert.Equal(t, "a", cx.idByAddr("x"), "idByAddr(\"a\")")
	assert.Equal(t, "b", cx.idByAddr("y"), "idByAddr(\"a\")")
	assert.Equal(t, "c", cx.idByAddr("z"), "idByAddr(\"a\")")
	assert.Equal(t, 0, cx.indexByAddr("x"), "indexByAddr(\"a\")")
	assert.Equal(t, 1, cx.indexByAddr("y"), "indexByAddr(\"a\")")
	assert.Equal(t, 2, cx.indexByAddr("z"), "indexByAddr(\"a\")")
}

func TestClusterNil(t *testing.T) {
	cx := newCluster("c", map[string]string{"a":"x", "b":"y", "c":"z", "":""})
	assert.Equal(t, 3, cx.Len(), "Len")
	assert.Equal(t, 2, cx.Quorum(), "Quorum")
	assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
}
