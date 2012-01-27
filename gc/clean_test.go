package gc

import (
	"github.com/bmizerany/assert"
	"github.com/ha/doozerd/store"
	"testing"
	"time"
)

func TestGcClean(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	ticker := make(chan time.Time)
	defer close(ticker)

	go Clean(st, 3, ticker)

	st.Ops <- store.Op{1, store.Nop}
	st.Ops <- store.Op{2, store.Nop}
	st.Ops <- store.Op{3, store.Nop}
	st.Ops <- store.Op{4, store.Nop}

	_, err := st.Wait(store.Any, 1)
	assert.Equal(t, nil, err)
	ticker <- time.Unix(0, 1)
	ticker <- time.Unix(0, 1) // Extra tick to ensure the last st.Clean has completed
	_, err = st.Wait(store.Any, 1)
	assert.Equal(t, store.ErrTooLate, err)
}
