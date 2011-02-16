package consensus

import (
	"doozer/store"
	"github.com/bmizerany/assert"
	"testing"
)


const (
	info = "/doozer/info"
	slot = "/doozer/slot"
)


func TestRunSimple(t *testing.T) {
	alpha := int64(3)
	runs := make(chan Run)
	st     := store.New()
	defer close(st.Ops)

	for i := int64(0); i < alpha; i++ {
		st.Ops <- store.Op{i, store.Nop}
	}

	go GenerateRuns(st, runs)

	st.Ops <- store.Op{
		Seqn: alpha+0,
		Mut:  store.MustEncodeSet(info+"/abc123/public-addr", "127.0.0.1:1234", 0),
	}

	st.Ops <- store.Op{
		Seqn: alpha+1,
		Mut: store.MustEncodeSet(slot+"/1", "abc123", 0),
	}

	assert.Equal(t, Run{}, <-runs)
}
