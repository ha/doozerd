package gc

import (
	"doozer/store"
)

func Clean(st *store.Store, keep int64, ticker <-chan int64) {
	for _ = range ticker {
		last := (<-st.Seqns) - keep
		st.Clean(last)
	}
}
