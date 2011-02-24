package server

import (
	"bytes"
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


func mustUnmarshal(b []byte) (r *R) {
	r = new(R)
	err := proto.Unmarshal(b, r)
	if err != nil {
		panic(err)
	}
	return
}


func TestDelNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
		tx:      make(map[int32]txn),
	}
	r := c.del(&T{}, txn{})
	assert.Equal(t, missingArg, r)
}


func TestDelSnapNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
		tx:      make(map[int32]txn),
	}
	r := c.delSnap(&T{}, txn{})
	assert.Equal(t, missingArg, r)
}


func TestCheckinNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
		tx:      make(map[int32]txn),
	}
	r := c.checkin(&T{}, txn{})
	assert.Equal(t, missingArg, r)
}


func TestSetNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
		tx:      make(map[int32]txn),
	}
	r := c.set(&T{}, txn{})
	assert.Equal(t, missingArg, r)
}

func TestServerCloseTxn(t *testing.T) {
	c := &conn{
		tx: make(map[int32]txn),
	}

	tx := newTxn()
	c.tx[1] = tx

	c.closeTxn(1, tx)

	assert.Equal(t, map[int32]txn{}, c.tx)
	assert.Equal(t, false, <-tx.done)
}


func TestServerCancel(t *testing.T) {
	var buf bytes.Buffer
	c := &conn{
		c:       &buf,
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
		tx:      make(map[int32]txn),
	}

	fakeTx := newTxn()
	c.tx[1] = fakeTx

	cancelTx := newTxn()
	c.tx[2] = cancelTx

	done := make(chan bool, 1)
	go func() {
		c.cancel(&T{Tag: proto.Int32(2), Id: proto.Int32(1)}, cancelTx)
		done <- true
	}()

	// fake verb
	<-fakeTx.cancel
	c.closeTxn(1, fakeTx) // standard thing to do for every verb

	<-done
	b := buf.Bytes()
	assert.NotEqual(t, []byte{}, b)
	assert.Equal(t, []byte{0, 0, 0, 4}, b[0:4])

	exp := &R{
		Tag:   proto.Int32(2),
		Flags: proto.Int32(Valid|Done),
	}
	assert.Equal(t, exp, mustUnmarshal(b[4:]))
}
