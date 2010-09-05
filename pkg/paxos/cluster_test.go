package paxos

import (
	"junta/assert"
	"testing"
)

func TestCluster(t *testing.T) {
	membership := map[string]string{"a":"x", "b":"y", "c":"z"}
	cx := newCluster(membership)
	cx.self = "c"
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

func TestClusterCopiesData(t *testing.T) {
	membership := map[string]string{"a":"x", "b":"y", "c":"z"}
	exp := map[string]string{}
	for k,v := range membership {
		exp[k] = v
	}
	cx := newCluster(membership)
	cx.self = "c"
	membership["d"] = "w"
	assert.Equal(t, exp, cx.addrsById)
}

func TestClusterNil(t *testing.T) {
	cx := newCluster(map[string]string{"a":"x", "b":"y", "c":"z", "":""})
	cx.self = "c"
	assert.Equal(t, 3, cx.Len(), "Len")
	assert.Equal(t, 2, cx.Quorum(), "Quorum")
	assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
}
