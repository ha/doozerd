package consensus

import (
	"doozer/store"
)


// DefRev is the rev in which this manager was defined;
// it will participate starting at DefRev+Alpha.
type Config struct {
	Self   string
	DefRev int64
	Alpha  int64
	In     <-chan Packet
	Out    chan<- Packet
	Ops    chan<- store.Op
	PSeqn  chan<- int64
	Props  <-chan *Prop
	TFill  int64
	Store  *store.Store
	Ticker <-chan int64
}


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
