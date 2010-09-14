package paxos

import (
	"junta/assert"
	"junta/store"
	"testing"
)

func TestRegistrarMembers(t *testing.T) {
	st := store.New()
	rg := NewRegistrar(st, 0, 2)
	go func() {
		go st.Apply(3, mustEncodeSet(membersKey+"/c", "1"))
		go st.Apply(2, mustEncodeSet(membersKey+"/b", "1"))
		go st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	}()

	members, active := rg.setsForSeqn(5)
	t.Logf("members for %d = %v", 5, members)
	assert.Equal(t, 3, len(members), "5 Len")
	assert.Equal(t, 0, len(active), "5 Len")

	members, active = rg.setsForSeqn(4)
	t.Logf("members for %d = %v", 4, members)
	assert.Equal(t, 2, len(members), "4 Len")
	assert.Equal(t, 0, len(active), "4 Len")

	members, active = rg.setsForSeqn(3)
	t.Logf("members for %d = %v", 3, members)
	assert.Equal(t, 1, len(members), "3 Len")
	assert.Equal(t, 0, len(active), "3 Len")

	members, active = rg.setsForSeqn(2)
	t.Logf("members for %d = %v", 2, members)
	assert.Equal(t, 1, len(members), "2 Len")
	assert.Equal(t, 0, len(active), "2 Len")

	members, active = rg.setsForSeqn(1)
	t.Logf("members for %d = %v", 1, members)
	assert.Equal(t, 1, len(members), "1 Len")
	assert.Equal(t, 0, len(active), "1 Len")
}

func TestRegistrarActive(t *testing.T) {
	st := store.New()
	rg := NewRegistrar(st, 0, 2)
	go func() {
		go st.Apply(3, mustEncodeSet(slotDir+"0", "c"))
		go st.Apply(2, mustEncodeSet(slotDir+"1", "b"))
		go st.Apply(1, mustEncodeSet(slotDir+"2", "a"))
	}()

	members, active := rg.setsForSeqn(5)
	t.Logf("members for %d = %v", 5, members)
	assert.Equal(t, 0, len(members), "5 Len")
	assert.Equal(t, []string{"a", "b", "c"}, active)

	members, active = rg.setsForSeqn(4)
	t.Logf("members for %d = %v", 4, members)
	assert.Equal(t, 0, len(members), "4 Len")
	assert.Equal(t, []string{"a", "b"}, active)

	members, active = rg.setsForSeqn(3)
	t.Logf("members for %d = %v", 3, members)
	assert.Equal(t, 0, len(members), "3 Len")
	assert.Equal(t, []string{"a"}, active)

	members, active = rg.setsForSeqn(2)
	t.Logf("members for %d = %v", 2, members)
	assert.Equal(t, 0, len(members), "2 Len")
	assert.Equal(t, []string{"a"}, active)

	members, active = rg.setsForSeqn(1)
	t.Logf("members for %d = %v", 1, members)
	assert.Equal(t, 0, len(members), "1 Len")
	assert.Equal(t, []string{"a"}, active)
}

func TestRegistrarEmptySlot(t *testing.T) {
	st := store.New()
	rg := NewRegistrar(st, 0, 2)
	go func() {
		go st.Apply(3, mustEncodeSet(slotDir+"0", "c"))
		go st.Apply(2, mustEncodeSet(slotDir+"1", ""))
		go st.Apply(1, mustEncodeSet(slotDir+"2", "a"))
	}()

	members, active := rg.setsForSeqn(5)
	t.Logf("members for %d = %v", 5, members)
	assert.Equal(t, 0, len(members), "5 Len")
	assert.Equal(t, []string{"a", "c"}, active)

	members, active = rg.setsForSeqn(4)
	t.Logf("members for %d = %v", 4, members)
	assert.Equal(t, 0, len(members), "4 Len")
	assert.Equal(t, []string{"a"}, active)

	members, active = rg.setsForSeqn(3)
	t.Logf("members for %d = %v", 3, members)
	assert.Equal(t, 0, len(members), "3 Len")
	assert.Equal(t, []string{"a"}, active)

	members, active = rg.setsForSeqn(2)
	t.Logf("members for %d = %v", 2, members)
	assert.Equal(t, 0, len(members), "2 Len")
	assert.Equal(t, []string{"a"}, active)

	members, active = rg.setsForSeqn(1)
	t.Logf("members for %d = %v", 1, members)
	assert.Equal(t, 0, len(members), "1 Len")
	assert.Equal(t, []string{"a"}, active)
}

func TestRegistrarSlotInitFirst(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(slotDir+"0", "a"))
	st.Sync(1)
	rg := NewRegistrar(st, 1, 0)

	members, active := rg.setsForVersion(1)
	assert.Equal(t, 0, len(members))
	assert.Equal(t, []string{"a"}, active)
}

func TestRegistrarSlotInitTwo(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(slotDir+"0", "b"))
	st.Apply(2, mustEncodeSet(slotDir+"1", "a"))
	rg := NewRegistrar(st, 2, 0)

	members, active := rg.setsForVersion(2)
	assert.Equal(t, 0, len(members))
	assert.Equal(t, []string{"a", "b"}, active)
}

func TestRegistrarInitFirst(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	st.Sync(1)
	rg := NewRegistrar(st, 1, 0)

	members, active := rg.setsForVersion(1)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, 0, len(active))
}

func TestRegistrarInitNext(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	st.Sync(1)
	rg := NewRegistrar(st, 1, 0)
	go func() {
		go st.Apply(2, mustEncodeSet(membersKey+"/b", "1"))
	}()

	members, active := rg.setsForVersion(2)
	assert.Equal(t, 2, len(members), "2 Len")
	assert.Equal(t, 0, len(active), "2 Len")

	members, active = rg.setsForVersion(1)
	assert.Equal(t, 1, len(members), "1 Len")
	assert.Equal(t, 0, len(active), "1 Len")
}

func TestRegistrarTooOld(t *testing.T) {
	st := store.New()
	st.Apply(1, mustEncodeSet(membersKey+"/a", "1"))
	st.Apply(2, mustEncodeSet(membersKey+"/a", "1"))
	st.Sync(2)
	rg := NewRegistrar(st, 2, 0)

	members, active := rg.setsForVersion(1)
	assert.Equal(t, map[string]string{}, members, "members 1")
	assert.Equal(t, []string{}, active, "active 1")
}
