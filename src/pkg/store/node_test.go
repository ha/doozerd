package store

import (
	"github.com/bmizerany/assert"
	"os"
	"testing"
)

func TestNodeApplySet(t *testing.T) {
	k, v, seqn, rev := "x", "a", int64(1), int64(1)
	p := "/" + k
	m := MustEncodeSet(p, v, Clobber)
	n, e := emptyDir.apply(seqn, m)
	exp := node{"", Dir, map[string]node{k: {v, rev, nil}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, p, v, rev, m, nil, n}, e)
}

func TestNodeApplyDel(t *testing.T) {
	k, seqn, rev := "x", int64(1), int64(1)
	r := node{"", Dir, map[string]node{k: {"a", rev, nil}}}
	p := "/" + k
	m := MustEncodeDel(p, rev)
	n, e := r.apply(seqn, m)
	assert.Equal(t, emptyDir, n)
	assert.Equal(t, Event{seqn, p, "", Missing, m, nil, n}, e)
}

func TestNodeApplyNop(t *testing.T) {
	seqn := int64(1)
	m := Nop
	n, e := emptyDir.apply(seqn, m)
	assert.Equal(t, emptyDir, n)
	assert.Equal(t, Event{seqn, "/", "", nop, m, nil, n}, e)
}

func TestNodeApplyBadMutation(t *testing.T) {
	seqn, rev := int64(1), int64(1)
	m := BadMutations[0]
	n, e := emptyDir.apply(seqn, m)
	exp := node{"", Dir, map[string]node{"ctl": {"", Dir, map[string]node{"err": {ErrBadMutation.String(), rev, nil}}}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, ErrorPath, ErrBadMutation.String(), rev, m, ErrBadMutation, n}, e)
}

func TestNodeApplyBadInstruction(t *testing.T) {
	seqn, rev := int64(1), int64(1)
	m := BadInstructions[0]
	n, e := emptyDir.apply(seqn, m)
	err := &BadPathError{""}
	exp := node{"", Dir, map[string]node{"ctl": {"", Dir, map[string]node{"err": {err.String(), rev, nil}}}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, ErrorPath, err.String(), rev, m, err, n}, e)
}

func TestNodeApplyRevMismatch(t *testing.T) {
	k, v, seqn, rev := "x", "a", int64(1), int64(1)
	p := "/" + k

	// -123 is less that the current rev, which is zero; and not Clobber.
	m := MustEncodeSet(p, v, -123)
	n, e := emptyDir.apply(seqn, m)

	exp := node{"", Dir, map[string]node{"ctl": {"", Dir, map[string]node{"err": {ErrRevMismatch.String(), rev, nil}}}}}
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{seqn, ErrorPath, ErrRevMismatch.String(), rev, m, ErrRevMismatch, n}, e)
}


func TestNodeNotADirectory(t *testing.T) {
	r, _ := emptyDir.apply(1, MustEncodeSet("/x", "a", Clobber))
	m := MustEncodeSet("/x/y", "b", Clobber)
	n, e := r.apply(2, m)
	exp, _ := r.apply(2, MustEncodeSet("/ctl/err", os.ENOTDIR.String(), Clobber))
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{2, ErrorPath, os.ENOTDIR.String(), 2, m, os.ENOTDIR, n}, e)
}

func TestNodeNotADirectoryDeeper(t *testing.T) {
	r, _ := emptyDir.apply(1, MustEncodeSet("/x", "a", Clobber))
	m := MustEncodeSet("/x/y/z/w", "b", Clobber)
	n, e := r.apply(2, m)
	exp, _ := r.apply(2, MustEncodeSet("/ctl/err", os.ENOTDIR.String(), Clobber))
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{2, ErrorPath, os.ENOTDIR.String(), 2, m, os.ENOTDIR, n}, e)
}

func TestNodeIsADirectory(t *testing.T) {
	r, _ := emptyDir.apply(1, MustEncodeSet("/x/y", "a", Clobber))
	m := MustEncodeSet("/x", "b", Clobber)
	n, e := r.apply(2, m)
	exp, _ := r.apply(2, MustEncodeSet("/ctl/err", os.EISDIR.String(), Clobber))
	assert.Equal(t, exp, n)
	assert.Equal(t, Event{2, ErrorPath, os.EISDIR.String(), 2, m, os.EISDIR, n}, e)
}
