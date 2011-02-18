package consensus

import (
	"fmt"
	"github.com/bmizerany/assert"
	"testing"
)

func (x *M_Cmd) Format(f fmt.State, c int) {
	if c == 'v' && f.Flag('#') && x != nil {
		fmt.Fprintf(f, "M_%s", M_Cmd_name[int32(*x)])
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
	fmt.Fprintf(f, s, (*int32)(x))
}

// For testing convenience
func newVote(i int64, vval string) *M {
	return &M{Cmd: vote, Vrnd: &i, Value: []byte(vval)}
}

// For testing convenience
func newVoteFrom(from string, i int64, vval string) packet {
	m := newVote(i, vval)
	m.SetSeqn(1)
	return packet{from, *m}
}

// For testing convenience
func newNominate(crnd int64, v string) *M {
	return &M{Cmd: nominate, Crnd: &crnd, Value: []byte(v)}
}

// For testing convenience
func newNominateSeqn1(crnd int64, v string) *M {
	m := newNominate(crnd, v)
	m.SetSeqn(1)
	return m
}

// For testing convenience
func newRsvp(i, vrnd int64, vval string) *M {
	return &M{
		Cmd:   rsvp,
		Crnd:  &i,
		Vrnd:  &vrnd,
		Value: []byte(vval),
	}
}

// For testing convenience
func newRsvpFrom(from string, i, vrnd int64, vval string) packet {
	m := newRsvp(i, vrnd, vval)
	m.SetSeqn(1)
	return packet{from, *m}
}

// For testing convenience
func newInvite(crnd int64) *M {
	return &M{Cmd: invite, Crnd: &crnd}
}

// For testing convenience
func newInviteSeqn1(rnd int64) *M {
	m := newInvite(rnd)
	m.SetSeqn(1)
	return m
}

// For testing convenience
func newPropose(val string) *M {
	return &M{Cmd: propose, Value: []byte(val)}
}

// For testing convenience
func newLearn(val string) *M {
	return &M{Cmd: learn, Value: []byte(val)}
}

func TestMessageSetSeqn(t *testing.T) {
	m := newInvite(1)
	m.SetSeqn(1)
	assert.Equal(t, int64(1), *m.Seqn, "")
	m.SetSeqn(2)
	assert.Equal(t, int64(2), *m.Seqn, "")
}
