package paxos

import (
	"net"
	"os"
	"testing"
	"testing/quick"

	"github.com/bmizerany/assert"
)

// For testing convenience
func newVoteFrom(from int, i uint64, vval string) Msg {
	m := newVote(i, vval)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newNominateFrom(from int, crnd uint64, v string) Msg {
	m := newNominate(crnd, v)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newRsvpFrom(from int, i, vrnd uint64, vval string) Msg {
	m := newRsvp(i, vrnd, vval)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

// For testing convenience
func newInviteFrom(from int, rnd uint64) Msg {
	m := newInvite(rnd)
	m.SetSeqn(1)
	m.SetFrom(from)
	return m
}

func TestMessageNewInvite(t *testing.T) {
	m := newInvite(1)
	assert.Equal(t, invite, m.Cmd(), "")
	crnd := inviteParts(m)
	assert.Equal(t, uint64(1), crnd, "")
}

func TestMessageNewInviteAlt(t *testing.T) {
	m := newInvite(2)
	assert.Equal(t, invite, m.Cmd(), "")
	crnd := inviteParts(m)
	assert.Equal(t, uint64(2), crnd, "")
}

func TestMessageNewNominate(t *testing.T) {
	m := newNominate(1, "foo")
	assert.Equal(t, nominate, m.Cmd(), "")
	crnd, v := nominateParts(m)
	assert.Equal(t, uint64(1), crnd, "")
	assert.Equal(t, "foo", v, "")
}

func TestMessageNewNominateAlt(t *testing.T) {
	m := newNominate(2, "bar")
	assert.Equal(t, nominate, m.Cmd(), "")
	crnd, v := nominateParts(m)
	assert.Equal(t, uint64(2), crnd, "")
	assert.Equal(t, "bar", v, "")
}

func TestMessageNewRsvp(t *testing.T) {
	m := newRsvp(1, 0, "")
	assert.Equal(t, rsvp, m.Cmd(), "")
	i, vrnd, vval := rsvpParts(m)
	assert.Equal(t, uint64(1), i, "")
	assert.Equal(t, uint64(0), vrnd, "")
	assert.Equal(t, "", vval, "")
}

func TestMessageNewRsvpAlt(t *testing.T) {
	m := newRsvp(2, 1, "foo")
	assert.Equal(t, rsvp, m.Cmd(), "")
	i, vrnd, vval := rsvpParts(m)
	assert.Equal(t, uint64(2), i, "")
	assert.Equal(t, uint64(1), vrnd, "")
	assert.Equal(t, "foo", vval, "")
}

func TestMessageNewVote(t *testing.T) {
	m := newVote(1, "foo")
	assert.Equal(t, vote, m.Cmd(), "")
	i, vval := voteParts(m)
	assert.Equal(t, uint64(1), i, "")
	assert.Equal(t, "foo", vval, "")
}

func TestMessageNewVoteAlt(t *testing.T) {
	m := newVote(2, "bar")
	assert.Equal(t, vote, m.Cmd(), "")
	i, vval := voteParts(m)
	assert.Equal(t, uint64(2), i, "")
	assert.Equal(t, "bar", vval, "")
}

func TestMessageNewLearn(t *testing.T) {
	f := func(exp string) bool {
		m := newLearn(exp)
		assert.Equal(t, learn, m.Cmd(), "")
		val := learnParts(m)
		assert.Equal(t, exp, val, "")
		return true
	}
	quick.Check(f, nil)
}

func TestMessageNewTick(t *testing.T) {
	m := newTick()
	assert.Equal(t, tick, m.Cmd(), "")
}

func TestMessageNewPropose(t *testing.T) {
	m := newPropose("foo")
	assert.Equal(t, propose, m.Cmd(), "")
	val := proposeParts(m)
	assert.Equal(t, "foo", val, "")
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
	assert.Equal(t, uint64(1), m.Seqn(), "")
	m.SetSeqn(2)
	assert.Equal(t, uint64(2), m.Seqn(), "")
}

func resize(m Msg, n int) Msg {
	x := len(m) + n
	if x > cap(m) {
		y := make(Msg, x)
		copy(y, m)
		return y
	}
	return m[0 : len(m)+n]
}

var badMessages = []Msg{
	{0},                            // too short
	{0, 255},                       // bad cmd
	resize(newInvite(0), -1),       // too short for type
	resize(newInvite(0), 1),        // too long for type
	resize(newRsvp(0, 0, ""), -1),  // too short for type
	resize(newNominate(0, ""), -1), // too short for type
	resize(newVote(0, ""), -1),     // too short for type
}

func TestBadMessagesOk(t *testing.T) {
	for _, m := range badMessages {
		if m.Ok() {
			t.Errorf("check failed for bad msg: %#v", m)
		}
	}
}

var goodMessages = []Msg{
	newInvite(1),
	newRsvp(2, 1, "foo"),
	newNominate(1, "foo"),
	newVote(1, "foo"),
}

func TestGoodMessagesOk(t *testing.T) {
	for _, m := range goodMessages {
		if !m.Ok() {
			t.Errorf("check failed for good msg: %#v", m)
		}
	}
}

func TestWireBytes(t *testing.T) {
	m := newInvite(1)
	b := m.WireBytes()
	assert.Equal(t, []byte(m[1:]), b, "")
	b[0] = 2
	assert.Equal(t, byte(2), m[1], "")
}

type fakePacketConn struct {
	a net.Addr
	b []byte
}

func (fpc *fakePacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err os.Error) {
	return copy(b, fpc.b), fpc.a, nil
}

func TestReadMsg(t *testing.T) {
	msg := newInvite(2)
	addr := &net.UDPAddr{net.IP{1, 2, 3, 4}, 123}
	fpc := &fakePacketConn{addr, msg.WireBytes()}
	gotMsg, gotAddr, err := ReadMsg(fpc, 3000)
	assert.Equal(t, nil, err, "")
	assert.Equal(t, msg, gotMsg, "")
	assert.Equal(t, addr.String(), gotAddr, "")
}

func TestFlags(t *testing.T) {
	msg := newInvite(1)
	for i := uint(0); i < 8; i++ {
		f := 1 << i
		assert.Equal(t, false, msg.HasFlags(f), "f=%d", f)
	}
	x := msg.SetFlags(Ack)
	assert.Equal(t, x, msg)
	for i := uint(0); i < 8; i++ {
		f := 1 << i
		assert.Equal(t, f == Ack, msg.HasFlags(f), "f=%d", f)
	}
	x = msg.ClearFlags(Ack)
	assert.Equal(t, x, msg)
	for i := uint(0); i < 8; i++ {
		f := 1 << i
		assert.Equal(t, false, msg.HasFlags(f), "f=%d", f)
	}
}

func TestDup(t *testing.T) {
	m := newInvite(1)
	o := m.Dup()
	assert.Equal(t, m, o)
	o.SetFlags(1)
	assert.NotEqual(t, m, o)
}
