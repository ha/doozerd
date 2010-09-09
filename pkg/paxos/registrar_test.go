package paxos

import (
	"junta/assert"
	"junta/store"
	"testing"
)

func TestRegistrar(t *testing.T) {
	st := store.New()
	rg := NewRegistrar(st, 0, 2)
	go func() {
		go st.Apply(3, mustEncodeSet(membersKey+"/c", "1"))
		go st.Apply(2, mustEncodeSet(membersKey+"/b", "1"))
		go st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	}()

	members := rg.membersFor(5)
	t.Logf("members for %d = %v", 5, members)
	assert.Equal(t, 3, len(members), "5 Len")

	members = rg.membersFor(4)
	t.Logf("members for %d = %v", 4, members)
	assert.Equal(t, 2, len(members), "4 Len")

	members = rg.membersFor(3)
	t.Logf("members for %d = %v", 3, members)
	assert.Equal(t, 1, len(members), "3 Len")

	members = rg.membersFor(2)
	t.Logf("members for %d = %v", 2, members)
	assert.Equal(t, 1, len(members), "2 Len")

	members = rg.membersFor(1)
	t.Logf("members for %d = %v", 1, members)
	assert.Equal(t, 1, len(members), "1 Len")
}

func TestRegistrarInitFirst(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	st.Sync(1)
	rg := NewRegistrar(st, 1, 0)

	members := rg.membersAt(1)
	assert.Equal(t, 1, len(members))
}

func TestRegistrarInitNext(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	st.Sync(1)
	rg := NewRegistrar(st, 1, 0)
	go func() {
		go st.Apply(2, mustEncodeSet(membersKey+"/b", "1"))
	}()

	members := rg.membersAt(2)
	assert.Equal(t, 2, len(members), "2 Len")

	members = rg.membersAt(1)
	assert.Equal(t, 1, len(members), "1 Len")
}

func TestRegistrarTooOld(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	st.Apply(2, mustEncodeSet(membersKey+"/a", "1"))
	st.Sync(2)
	rg := NewRegistrar(st, 2, 0)

	members := rg.membersAt(1)
	assert.Equal(t, map[string]string{}, members, "members 1")
}
