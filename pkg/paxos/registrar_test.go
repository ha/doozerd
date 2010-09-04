package paxos

import (
	"junta/assert"
	"junta/store"
	"testing"
)

func TestRegistrar(t *testing.T) {
	st := store.New()
	rg := NewRegistrar("a", st, 2)
	go func() {
		go st.Apply(3, mustEncodeSet(membersKey+"/c", "1"))
		go st.Apply(2, mustEncodeSet(membersKey+"/b", "1"))
		go st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
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

func TestRegistrarHistory(t *testing.T) {
	exp := []map[string]string{
		map[string]string{"a":"x"},
		map[string]string{"a":"x", "b":"y"},
		map[string]string{"a":"x", "b":"y", "c":"z"},
	}

	st := store.New()
	rg := NewRegistrar("a", st, 2)
	go func() {
		st.Apply(1, mustEncodeSet(membersKey+"/a", "x"))
		st.Apply(2, mustEncodeSet(membersKey+"/b", "y"))
		st.Apply(3, mustEncodeSet(membersKey+"/c", "z"))
	}()

	h := rg.History(1, 4)
	assert.Equal(t, exp, h)
}
