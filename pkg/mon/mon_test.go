package mon

import (
	"junta/assert"
	"junta/store"
	"os"
	"testing"
)

type applyer struct {
	st *store.Store
	seqn uint64
}

func (a *applyer) set(name, value, cas string) {
	a.seqn++
	mut := store.MustEncodeSet(name, value, cas)
	a.st.Apply(a.seqn, mut)
}

func (a *applyer) add(name, value string) {
	a.set(name, value, store.Missing)
}

type fakeClient chan string

func (fc fakeClient) Set(path, body, cas string) (seqn uint64, err os.Error) {
	mut, err := store.EncodeSet(path, body, cas)
	if err != nil {
		return 0, err
	}

	fc <- mut
	return 0, nil
}

func (fc fakeClient) Del(path, cas string) (seqn uint64, err os.Error) {
	mut, err := store.EncodeDel(path, cas)
	if err != nil {
		return 0, err
	}

	fc <- mut
	return 0, nil
}

func TestRunOne(t *testing.T) {
	var exp string
	p := "/a"
	ap := applyer{st:store.New()}
	cl := make(fakeClient)
	go func() {
		panic(Monitor("a", p, ap.st, cl))
	}()

	ap.add(defDir+"a.service/service/exec-start", "/bin/true")
	ap.add(ctlDir+"a.service", "start")

	exp = store.MustEncodeSet(p+lockDir+"a.service", "a", store.Missing)
	assert.Equal(t, exp, <-cl)
	ap.add(lockDir+"a.service", "a")

	exp = store.MustEncodeSet(p+statusDir+"a.service/node", "a", store.Clobber)
	assert.Equal(t, exp, <-cl)

	exp = store.MustEncodeSet(p+statusDir+"a.service/host", os.Getenv("HOSTNAME"), store.Clobber)
	assert.Equal(t, exp, <-cl)
	exp = store.MustEncodeSet(p+statusDir+"a.service/status", "up", store.Clobber)
	assert.Equal(t, exp, <-cl)
	exp = store.MustEncodeDel(p+statusDir+"a.service/reason", store.Clobber)
	assert.Equal(t, exp, <-cl)

	<-cl // set pid

	exp = store.MustEncodeDel(p+statusDir+"a.service/pid", store.Clobber)
	assert.Equal(t, exp, <-cl)

	exp = store.MustEncodeSet(p+statusDir+"a.service/status", "down", store.Clobber)
	assert.Equal(t, exp, <-cl)
}
