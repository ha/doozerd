package paxos

import (
	"junta/assert"
	"testing"
)

func TestCluster(t *testing.T) {
	cx := newCluster("c", []string{"a", "b", "c"})
	assert.Equal(t, 3, cx.Len(), "Len")
	assert.Equal(t, 2, cx.Quorum(), "Quorum")
	assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
}

func TestClusterNil(t *testing.T) {
	cx := newCluster("c", []string{"a", "b", "c", ""})
	assert.Equal(t, 3, cx.Len(), "Len")
	assert.Equal(t, 2, cx.Quorum(), "Quorum")
	assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
}
