package consensus

import (
	"fmt"
	"github.com/bmizerany/assert"
	"github.com/kr/pretty.go"
	"testing"
)

func (x M_Cmd) Format(f fmt.State, c int) {
	if c == 'v' && f.Flag('#') {
		fmt.Fprintf(f, "M_%s", M_Cmd_name[int32(x)])
		return
	}

	s := "%"
	for i := 0; i < 128; i++ {
		if f.Flag(i) {
			s += string(i)
		}
	}
	if w, ok := f.Width(); ok {
		s += fmt.Sprintf("%d", w)
	}
	if p, ok := f.Precision(); ok {
		s += fmt.Sprintf(".%d", p)
	}
	s += string(c)
	fmt.Fprintf(f, s, int32(x))
}

// For testing convenience
func newVote(i int64, vval string) *M {
	return &M{WireCmd: vote, Vrnd: &i, Value: []byte(vval)}
}

// For testing convenience
func newVoteFrom(from int32, i int64, vval string) *M {
	m := newVote(i, vval)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newNominate(crnd int64, v string) *M {
	return &M{WireCmd: nominate, Crnd: &crnd, Value: []byte(v)}
}

// For testing convenience
func newNominateFrom(from int32, crnd int64, v string) *M {
	m := newNominate(crnd, v)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newRsvp(i, vrnd int64, vval string) *M {
	return &M{
		WireCmd:  rsvp,
		Crnd:     &i,
		Vrnd:     &vrnd,
		Value:    []byte(vval),
	}
}

// For testing convenience
func newRsvpFrom(from int32, i, vrnd int64, vval string) *M {
	m := newRsvp(i, vrnd, vval)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newInvite(crnd int64) *M {
	return &M{WireCmd: invite, Crnd: &crnd}
}

// For testing convenience
func newInviteFrom(from int32, rnd int64) *M {
	m := newInvite(rnd)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newPropose(val string) *M {
	return &M{WireCmd: propose, Value: []byte(val)}
}

// For testing convenience
func newLearn(val string) *M {
	return &M{WireCmd: learn, Value: []byte(val)}
}

func TestMessageSetFrom(t *testing.T) {
	m := newInvite(1)
	m.SetFrom(1)
	assert.Equal(t, 1, m.From(), "")
	m.SetFrom(2)
	assert.Equal(t, 2, m.From(), "")
}

func TestMessageSetSeqn(t *testing.T) {
	m := newInvite(1)
	m.SetSeqn(1)
	assert.Equal(t, int64(1), m.Seqn(), "")
	m.SetSeqn(2)
	assert.Equal(t, int64(2), m.Seqn(), "")
}

var badMessages = []*M{
	&M{},               // no cmd
	&M{WireCmd: learn}, // no seqn

	&M{WireCmd: invite, WireSeqn: new(int64)}, // no crnd
	&M{WireCmd: nominate, WireSeqn: new(int64)}, // no crnd
	&M{WireCmd: vote, WireSeqn: new(int64)}, // no vrnd

	&M{WireCmd: rsvp, WireSeqn: new(int64), Crnd: new(int64)}, // no vrnd
	&M{WireCmd: rsvp, WireSeqn: new(int64), Vrnd: new(int64)}, // no crnd
}

func TestBadMessagesOk(t *testing.T) {
	for _, m := range badMessages {
		if m.Ok() {
			t.Errorf("check failed for bad msg: %#v", m)
		}
	}
}

var goodMessages = []*M{
	newInviteFrom(0, 1),
	newRsvpFrom(0, 2, 1, "foo"),
	newNominateFrom(0, 1, "foo"),
	newVoteFrom(0, 1, "foo"),
}

func TestGoodMessagesOk(t *testing.T) {
	for _, m := range goodMessages {
		if !m.Ok() {
			t.Errorf("check failed for good msg: %# v", pretty.Formatter(m))
		}
	}
}

func TestDup(t *testing.T) {
	m := newInvite(1)
	o := m.Dup()
	assert.Equal(t, m, o)
	o.SetSeqn(2)
	assert.NotEqual(t, m, o)
}
