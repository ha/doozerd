package store

import (
	"junta/assert"
	"testing"
)

func TestNodeApplySet(t *testing.T) {
	k, v, seqn, cas := "x", "a", uint64(1), "1"
	p := "/"+k
	m := MustEncodeSet(p, v, Clobber)
	n, e := root.apply(seqn, m)
	exp := node{"", Dir, map[string]node{k:node{v, cas, nil}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, p, v, cas, m, nil}, e)
}

func TestNodeApplyDel(t *testing.T) {
	k, seqn, cas := "x", uint64(1), "1"
	r := node{"", Dir, map[string]node{k:node{"a", cas, nil}}}
	p := "/"+k
	m := MustEncodeDel(p, cas)
	n, e := r.apply(seqn, m)
	assert.Equal(t, root, n)
	assert.Equal(t, Event{seqn, p, "", Missing, m, nil}, e)
}

func TestNodeApplyBadMutation(t *testing.T) {
	seqn := uint64(1)
	m := BadMutations[0]
	n, e := root.apply(seqn, m)
	assert.Equal(t, root, n)
	expPath := "/store/error"
	assert.Equal(t, Event{seqn, expPath, "", "", m, BadMutationError}, e)
}

func TestNodeApplyBadInstruction(t *testing.T) {
	seqn := uint64(1)
	m := BadInstructions[0]
	n, e := root.apply(seqn, m)
	assert.Equal(t, root, n)
	expPath := "/store/error"
	assert.Equal(t, Event{seqn, expPath, "", "", m, BadPathError}, e)
}

func TestNodeApplyCasMismatch(t *testing.T) {
	k, v, seqn := "x", "a", uint64(1)
	p := "/"+k
	m := MustEncodeSet(p, v, "123")
	n, e := root.apply(seqn, m)
	assert.Equal(t, root, n)
	expPath := "/store/error"
	assert.Equal(t, Event{seqn, expPath, "", "", m, CasMismatchError}, e)
}
