package store

import (
	"bytes"
	"gob"
	"io"
	"junta/assert"
	"os"
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

func TestNodeApplyNop(t *testing.T) {
	seqn := uint64(1)
	m := Nop
	n, e := root.apply(seqn, m)
	assert.Equal(t, root, n)
	assert.Equal(t, Event{seqn, "", "", "", m, nil}, e)
}

func TestNodeApplyBadMutation(t *testing.T) {
	seqn, cas := uint64(1), "1"
	m := BadMutations[0]
	n, e := root.apply(seqn, m)
	exp := node{"", Dir, map[string]node{"store":node{"", Dir, map[string]node{"error":node{ErrBadMutation.String(), cas, nil}}}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, ErrorPath, ErrBadMutation.String(), cas, m, ErrBadMutation}, e)
}

func TestNodeApplyBadInstruction(t *testing.T) {
	seqn, cas := uint64(1), "1"
	m := BadInstructions[0]
	n, e := root.apply(seqn, m)
	exp := node{"", Dir, map[string]node{"store":node{"", Dir, map[string]node{"error":node{ErrBadPath.String(), cas, nil}}}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, ErrorPath, ErrBadPath.String(), cas, m, ErrBadPath}, e)
}

func TestNodeApplyCasMismatch(t *testing.T) {
	k, v, seqn, cas := "x", "a", uint64(1), "1"
	p := "/"+k
	m := MustEncodeSet(p, v, "123")
	n, e := root.apply(seqn, m)
	exp := node{"", Dir, map[string]node{"store":node{"", Dir, map[string]node{"error":node{ErrCasMismatch.String(), cas, nil}}}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, ErrorPath, ErrCasMismatch.String(), cas, m, ErrCasMismatch}, e)
}

func TestNodeSnapshotApply(t *testing.T) {
	s1 := New()
	mut1, _ := EncodeSet("/x", "a", Clobber)
	mut2, _ := EncodeSet("/x", "b", Clobber)
	s1.Apply(1, mut1)
	s1.Apply(2, mut2)
	s1.Sync(2)
	_, m := s1.Snapshot()

	n, e := root.apply(1, m)
	exp := node{"", Dir, map[string]node{"x":node{"b", "2", nil}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{2, "", "", "", m, nil}, e)
}

func TestNodeSnapshotBad(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	gob.NewEncoder(buf).Encode(uint64(1))
	seqnPart := buf.String()

	buf = bytes.NewBuffer([]byte{})
	gob.NewEncoder(buf).Encode(root)
	valPart := buf.String()
	valPart = valPart[0:len(valPart)/2]

	m := seqnPart+valPart
	n, e := root.apply(1, m)
	assert.Equal(t, root, n)
	assert.Equal(t, Event{2, "", "", "", m, io.ErrUnexpectedEOF}, e)
}

func TestNodeNotADirectory(t *testing.T) {
	r, _ := root.apply(1, MustEncodeSet("/x", "a", Clobber))
	m := MustEncodeSet("/x/y", "b", Clobber)
	n, e := r.apply(2, m)
	exp, _ := r.apply(2, MustEncodeSet("/store/error", os.ENOTDIR.String(), Clobber))
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{2, ErrorPath, os.ENOTDIR.String(), "2", m, os.ENOTDIR}, e)
}

func TestNodeNotADirectoryDeeper(t *testing.T) {
	r, _ := root.apply(1, MustEncodeSet("/x", "a", Clobber))
	m := MustEncodeSet("/x/y/z/w", "b", Clobber)
	n, e := r.apply(2, m)
	exp, _ := r.apply(2, MustEncodeSet("/store/error", os.ENOTDIR.String(), Clobber))
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{2, ErrorPath, os.ENOTDIR.String(), "2", m, os.ENOTDIR}, e)
}

func TestNodeIsADirectory(t *testing.T) {
	r, _ := root.apply(1, MustEncodeSet("/x/y", "a", Clobber))
	m := MustEncodeSet("/x", "b", Clobber)
	n, e := r.apply(2, m)
	exp, _ := r.apply(2, MustEncodeSet("/store/error", os.EISDIR.String(), Clobber))
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{2, ErrorPath, os.EISDIR.String(), "2", m, os.EISDIR}, e)
}
