package consensus

import (
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


const (
	info = "/doozer/info"
	slot = "/doozer/slot"
)


type msgSlot struct {
	*M
}


func (ms msgSlot) Put(m *M) {
	*ms.M = *m
}


func TestGetCals(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{Seqn: 1, Mut: store.MustEncodeSet(slot+"/1", "a", 0)}
	st.Ops <- store.Op{Seqn: 2, Mut: store.MustEncodeSet(slot+"/2", "c", 0)}
	st.Ops <- store.Op{Seqn: 3, Mut: store.MustEncodeSet(slot+"/3", "b", 0)}
	<-st.Seqns

	assert.Equal(t, []string{"a", "b", "c"}, getCals(st))
}


func alphaTest(t *testing.T, alpha int64) {
	runs  := make(chan Run)
	st    := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(info+"/a/public-addr", "127.0.0.1:1234", 0),
	}

	st.Ops <- store.Op{
		Seqn: 2,
		Mut: store.MustEncodeSet(slot+"/1", "a", 0),
	}

	for 2 != <-st.Seqns {}

	go GenerateRuns(alpha, st.Watch(store.Any), runs)

	// The only way to generate a run is on an event.  Send a noop here
	// to poke get things started
	st.Ops <- store.Op{3, store.Nop}

	assert.Equal(t, Run{Seqn: 3+alpha, Cals: []string{"a"}}, <-runs)
}


func TestRunAlphaOfOne(t *testing.T) {
	alphaTest(t, 1)
}


func TestRunAlphaOfThree(t *testing.T) {
	alphaTest(t, 3)
}


func TestRunAlphaOfFifty(t *testing.T) {
	alphaTest(t, 50)
}


func TestRunAfterWatch(t *testing.T) {
	alpha := int64(3)
	runs  := make(chan Run)
	st    := store.New()
	defer close(st.Ops)

	st.Ops <- store.Op{
		Seqn: 1,
		Mut:  store.MustEncodeSet(info+"/b/public-addr", "127.0.0.1:1234", 0),
	}

	for 1 != <-st.Seqns {}

	go GenerateRuns(alpha, st.Watch(store.Any), runs)

	st.Ops <- store.Op{
		Seqn: 2,
		Mut: store.MustEncodeSet(slot+"/1", "b", 0),
	}

	assert.Equal(t, Run{Seqn: 2+alpha, Cals: []string{"b"}}, <-runs)
}


func TestRunVoteDeliverd(t *testing.T) {
	r := Run{}
	r.learner.init(1)

	p := Packet{
		M:    M{
			WireSeqn: proto.Int64(1),
			WireCmd:  vote,
			Vrnd:     proto.Int64(1),
			Value:    []byte("foo"),
		},
		Addr: "X",
	}

	r.Deliver(p)

	assert.Equal(t, true, r.learner.done)
	assert.Equal(t, "foo", r.learner.v)
}


func TestRunInviteDeliverd(t *testing.T) {
	var r Run
	r.out = make(chan Packet, 100)

	r.Deliver(Packet{M: *newInviteFrom(1, 1)})

	assert.Equal(t, int64(1), r.acceptor.rnd)
}


func TestRunProposeDeliverd(t *testing.T) {
	var r Run
	r.out = make(chan Packet, 100)

	r.Deliver(Packet{M: M{WireCmd: propose}})
	assert.Equal(t, true, r.coordinator.begun)
}


func TestRunSendsCoordPacket(t *testing.T) {
	c := make(chan Packet, 100)
	var r Run
	r.coordinator.crnd = 1
	r.out = c

	r.Deliver(Packet{M: *newPropose("foo")})
	assert.Equal(t, *newInvite(1), (<-c).M)
}


func TestRunSendsAcceptorPacket(t *testing.T) {
	c := make(chan Packet, 100)
	var r Run
	r.out = c

	r.Deliver(Packet{M: *newInviteFrom(1, 1)})
	assert.Equal(t, *newRsvp(1, 0, ""), (<-c).M)
}
