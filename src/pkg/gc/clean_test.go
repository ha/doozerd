package gc

import (
	"doozer/store"
	"github.com/bmizerany/assert"
	"testing"
)

func TestGcClean(t *testing.T) {
	st := store.New()
	defer close(st.Ops)

	go Clean(st)
	for <-st.Watches < 1 {
	} // Wait for Clean()'s Watch to take

	st.Ops <- store.Op{1, store.Nop}
	st.Ops <- store.Op{2, store.MustEncodeSet("/doozer/slot/1", "a", store.Missing)}
	st.Ops <- store.Op{3, store.MustEncodeSet("/doozer/info/a/applied", "2", store.Missing)}

	st.Ops <- store.Op{4, store.MustEncodeSet("/doozer/info/X/applied", "0", store.Missing)}
	ch := st.Watch(store.MustCompileGlob("/x"))
	st.Ops <- store.Op{5, store.MustEncodeSet("/x", "", store.Missing)}
	<-ch
	close(ch)

	ev := <-st.Wait(1)
	assert.Equal(t, store.ErrTooLate, (ev).Err)
}
