package paxos

import (
	"junta/assert"
	"junta/store"
	"testing"
)

func TestRegistrar(t *testing.T) {
	st := store.New(logger)
	rg := newRegistrar("a", st, 2)
	go func() {
		st.Apply(3, mustEncodeSet("/b/junta/members/c", "1"))
		st.Apply(2, mustEncodeSet("/b/junta/members/b", "1"))
		st.Apply(1, mustEncodeSet("/b/junta/members/a", "1"))
	}()

	cx := rg.clusterFor(5)
	assert.Equal(t, 3, cx.Len(), "5 Len")

	cx = rg.clusterFor(4)
	assert.Equal(t, 2, cx.Len(), "4 Len")

	cx = rg.clusterFor(3)
	assert.Equal(t, 1, cx.Len(), "3 Len")

	cx = rg.clusterFor(2)
	assert.Equal(t, 1, cx.Len(), "2 Len")

	cx = rg.clusterFor(1)
	assert.Equal(t, 1, cx.Len(), "1 Len")
}
