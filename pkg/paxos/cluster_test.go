package paxos

import (
	"borg/assert"
	"testing"
)

func TestCluster(t *testing.T) {
	cx := NewCluster("c", []string{"a", "b", "c"})
	assert.Equal(t, 3, cx.Len(), "Len")
	assert.Equal(t, 2, cx.Quorum(), "Quorum")
	assert.Equal(t, 2, cx.SelfIndex(), "SelfIndex")
}
