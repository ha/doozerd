package consensus

import (
	"github.com/ha/doozerd/store"
)

type Proposer interface {
	Propose(v []byte) store.Event
}

func Set(p Proposer, path string, body []byte, rev int64) (e store.Event) {
	e.Mut, e.Err = store.EncodeSet(path, string(body), rev)
	if e.Err != nil {
		return
	}

	return p.Propose([]byte(e.Mut))
}

func Del(p Proposer, path string, rev int64) (e store.Event) {
	e.Mut, e.Err = store.EncodeDel(path, rev)
	if e.Err != nil {
		return
	}

	return p.Propose([]byte(e.Mut))
}
